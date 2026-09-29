WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        log_ts,
        attr_key,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER w_grp_2 AS event_seq_2,
        LAST_VALUE(attr_key) IGNORE NULLS OVER w_recent AS last_known_attr_key
    FROM flattened
    WINDOW w_grp AS (PARTITION BY YEAR(log_ts), service ORDER BY log_ts, id, attr_key), w_grp_2 AS (PARTITION BY YEAR(log_ts), attr_key ORDER BY log_ts DESC, id, attr_key), w_recent AS (PARTITION BY YEAR(log_ts), attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled,
        CONCAT(service, ' ', attr_key) AS service_attr_key
    FROM metrics
    WHERE attr_key IS NOT NULL
      AND log_ts > TIMESTAMP '2025-06-01 07:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY event_seq NULLS LAST) AS event_seq_cume_dist,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY TO_DATE(log_ts), attr_key), 4) AS event_seq_share,
        STDDEV_SAMP(event_seq) OVER w_grp AS log_ts_period_stddev_samp_event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY DATE_TRUNC('week', log_ts), attr_key), 4) AS event_seq_share_2,
        COUNT(event_seq_2) OVER (PARTITION BY attr_key) AS attr_key_count_event_seq_2
    FROM base
    WHERE event_seq_2 < 5
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('week', log_ts), attr_key)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY service, attr_key ORDER BY log_ts DESC) AS row_num
    FROM scored
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    CASE WHEN log_ts_period_stddev_samp_event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS log_ts_period_stddev_samp_event_seq_tier,
    MIN(log_ts) AS started_at,
    MAX(log_ts) AS ended_at,
    GROUPING_ID() AS grouping_id
FROM deduped
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', log_ts), CASE WHEN log_ts_period_stddev_samp_event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('DAY', log_ts)), ())
ORDER BY activity_day DESC NULLS LAST

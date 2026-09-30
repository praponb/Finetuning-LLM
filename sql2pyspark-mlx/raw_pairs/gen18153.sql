WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) AS rank_in_group
        FROM flattened
    ) ranked
    WHERE rank_in_group = 1
),
flagged AS (
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
sessionized AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        value,
        rank_in_group,
        SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    SELECT
        id,
        attr_key,
        value,
        rank_in_group,
        session_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY rank_in_group DESC, id ASC, attr_key ASC) AS row_num
        FROM sessionized
    ) x
    WHERE row_num = 1
)
SELECT
    CASE WHEN session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    ROUND(AVG(session_no), 2) AS avg_session_no,
    ROUND(AVG(session_no), 2) AS avg_session_no_2,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(attr_key = 'version') AS version_events
FROM latest
GROUP BY CASE WHEN session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END

WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY service, k ORDER BY log_ts, id, k) row_seq,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts, id, k) row_seq_2,
        FIRST_VALUE(k) IGNORE NULLS OVER w_running first_known_k,
        ROW_NUMBER() OVER w_recent seq_no,
        ROW_NUMBER() OVER w_hist event_seq
    FROM exploded
    WINDOW w_running AS (ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY log_ts DESC, id, k), w_hist AS (PARTITION BY host ORDER BY log_ts, id, k)
),
matched_keys AS (
    SELECT id, service
    FROM enriched
    EXCEPT
    SELECT id, service
    FROM app_logs
    WHERE service = 'auth'
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
),
scored AS (
    SELECT
        *,
        AVG(row_seq_2) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) row_seq_2_avg_30m,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('month', log_ts) ORDER BY row_seq NULLS LAST) row_seq_dense_rank,
        MAX(row_seq_2) OVER (PARTITION BY TO_DATE(log_ts), k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) row_seq_2_max_15m,
        PERCENT_RANK() OVER (ORDER BY row_seq DESC) row_seq_percent_rank,
        AVG(event_seq) OVER w_recent event_seq_avg_15m
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY service, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
enriched_app_logs AS (
    SELECT
        service,
        host,
        attr_value,
        row_seq_2,
        first_known_k,
        event_seq,
        row_seq_2_avg_30m,
        row_seq_percent_rank,
        event_seq_avg_15m,
        SUM(event_seq) OVER w_ordered event_seq_sum_15m,
        MAX(event_seq_avg_15m) OVER w_part event_seq_avg_15m_max_2h,
        RANK() OVER (ORDER BY row_seq DESC) row_seq_rank,
        COUNT(event_seq) OVER w_part event_seq_count_2h,
        AVG(row_seq) OVER (PARTITION BY k) k_avg_row_seq
    FROM scored
    WINDOW w_ordered AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
stats AS (
    SELECT
        CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
        MAX(event_seq) FILTER (WHERE service = 'billing') AS billing_max_event_seq,
        COUNT(DISTINCT host) AS distinct_hosts,
        COUNT(DISTINCT host) AS distinct_hosts_2,
        SIZE(COLLECT_SET(service)) AS num_services,
        SIZE(COLLECT_SET(host)) AS num_hosts
    FROM enriched_app_logs
    GROUP BY CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    event_seq_tier,
    billing_max_event_seq,
    distinct_hosts,
    num_hosts
FROM stats
ORDER BY event_seq_tier, num_hosts

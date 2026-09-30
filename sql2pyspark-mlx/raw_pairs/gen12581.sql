WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
valid AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        attr_value,
        CASE WHEN service = 'billing' THEN 1 ELSE 0 END AS is_billing,
        TRIM(host) AS clean_host
    FROM flattened
    WHERE log_ts >= TIMESTAMP '2025-06-01 14:00:00'
      AND key IS NOT NULL
),
scored AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        attr_value,
        SUM(is_billing) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_billing,
        MIN(is_billing) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_billing,
        AVG(is_billing) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_is_billing,
        SUM(is_billing) OVER (PARTITION BY key, service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_billing_sum_1h
    FROM valid
    WHERE log_ts < '2024-01-01'
),
scored_app_logs AS (
    SELECT
        *,
        LAG(running_sum_is_billing) OVER (PARTITION BY key ORDER BY latency_ms ASC, id ASC, key ASC) AS prev_running_sum_is_billing,
        LAG(latency_ms) OVER (PARTITION BY key ORDER BY latency_ms ASC, id ASC, key ASC) AS prev_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY latency_ms ASC, id ASC, key ASC) AS seq_no
    FROM scored
),
remaining_keys AS (
    SELECT id
    FROM scored_app_logs
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE latency_ms = 1056
),
kept_rows AS (
    SELECT s.*
    FROM scored_app_logs s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT *
FROM (
    SELECT
        key,
        service,
        running_sum_is_billing
    FROM kept_rows
) src
PIVOT (
    AVG(running_sum_is_billing) AS value_sum,
    COUNT(1) AS item_count
    FOR service IN ('checkout' AS checkout, 'auth' AS auth, 'search' AS search)
);

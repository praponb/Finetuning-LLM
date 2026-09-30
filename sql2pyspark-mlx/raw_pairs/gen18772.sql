WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k, service ORDER BY log_ts DESC, id, k) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        HOUR(log_ts) AS log_ts_hour,
        CAST(status_code AS DOUBLE) AS status_code_num
    FROM ranked
    WHERE log_ts >= TIMESTAMP '2025-06-01 14:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY level, k ORDER BY log_ts, id, k) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_num DESC, id, k) AS seq_no_2
    FROM filtered
    WHERE status_code >= 372
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        k,
        v,
        status_code_num,
        seq_no,
        seq_no_2,
        LEAD(seq_no_2) OVER (PARTITION BY level, k ORDER BY seq_no_2, id, k) AS next_seq_no_2,
        LAG(seq_no_2) OVER (PARTITION BY level, k ORDER BY seq_no_2, id, k) AS prev_seq_no_2,
        LEAD(seq_no) OVER (PARTITION BY level, k ORDER BY seq_no_2, id, k) AS next_seq_no,
        ROW_NUMBER() OVER (PARTITION BY level, k ORDER BY seq_no_2, id, k) AS seq_no_3
    FROM enriched
)
SELECT
    id,
    status_code,
    k,
    v,
    status_code_num,
    seq_no,
    next_seq_no_2,
    prev_seq_no_2,
    next_seq_no,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'EXTENDED' WHEN k RLIKE '^(region)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS k_zone
FROM metrics
ORDER BY seq_no DESC NULLS FIRST, next_seq_no;

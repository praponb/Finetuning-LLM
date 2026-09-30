WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts DESC, id, key) AS dedup_rank
    FROM exploded
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        key,
        value,
        dedup_rank,
        COUNT(status_code) OVER (PARTITION BY MONTH(log_ts), service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_count_1h,
        MAX(status_code) OVER (PARTITION BY level, key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_1h
    FROM ranked
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE TO_DATE(log_ts) < '2024-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    key,
    value,
    dedup_rank,
    status_code_count_1h,
    status_code_max_1h
FROM kept_rows
ORDER BY status_code_count_1h DESC NULLS FIRST;

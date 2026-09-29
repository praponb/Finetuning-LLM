WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        attr_key,
        status_code_high,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        AVG(latency_ms) OVER (PARTITION BY attr_key, service ORDER BY status_code_high ASC, id ASC, attr_key ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_5_prior,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY service ORDER BY status_code_high ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY status_code_high DESC, id ASC, attr_key ASC) AS row_seq,
        LAG(status_code, 1) OVER (PARTITION BY attr_key, service ORDER BY status_code_high ASC, id ASC, attr_key ASC) AS prev_status_code
    FROM parsed
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        attr_key,
        status_code_high,
        latency_ms_share,
        latency_ms_avg_5_prior,
        last_known_latency_ms,
        row_seq,
        prev_status_code * latency_ms_share AS prev_status_code_times_latency_ms_share
    FROM enriched
    WHERE attr_key IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        attr_key,
        latency_ms_share,
        latency_ms_avg_5_prior,
        last_known_latency_ms,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms_avg_5_prior DESC NULLS LAST, id ASC, attr_key ASC) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        attr_key,
        latency_ms_avg_5_prior,
        last_known_latency_ms,
        row_seq,
        UPPER(attr_key) AS attr_key_upper
    FROM ranked
    WHERE row_seq >= 4
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(row_seq) OVER (PARTITION BY attr_key) AS attr_key_count_row_seq,
        LAST_VALUE(last_known_latency_ms, TRUE) OVER (PARTITION BY attr_key ORDER BY last_known_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_latency_ms,
        LAST_VALUE(attr_key, TRUE) OVER (ORDER BY last_known_latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key
    FROM cleaned
)
SELECT
    id,
    attr_key,
    latency_ms_avg_5_prior,
    row_seq,
    attr_key_upper,
    attr_key_count_row_seq,
    last_known_last_known_latency_ms,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'CORE' WHEN attr_key RLIKE '^(trace)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS attr_key_zone
FROM enriched_app_logs

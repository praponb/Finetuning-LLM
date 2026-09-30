WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        latency_ms,
        attr_key,
        flag_bit1,
        ROUND(flag_bit1 * 0.9, 2) AS flag_bit1_scaled,
        ROUND(status_code_high / status_code, 2) AS status_code_high_per_status_code
    FROM flattened_attrs
    WHERE attr_key IS NOT NULL
      AND attr_key IN ('trace', 'region')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        latency_ms,
        attr_key,
        flag_bit1,
        status_code_high_per_status_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY flag_bit1_scaled DESC, id, attr_key) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY flag_bit1, id, attr_key) AS seq_no,
        NTILE(4) OVER (ORDER BY status_code_high_per_status_code DESC, id, attr_key) AS status_code_high_per_status_code_bucket
    FROM ranked
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    seq_no,
    CASE WHEN status_code_high_per_status_code_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_per_status_code_bucket_tier,
    MAX(status_code_high_per_status_code_bucket) AS max_status_code_high_per_status_code_bucket,
    PERCENTILE_APPROX(status_code_high_per_status_code_bucket, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_per_status_code_bucket_quartiles,
    ROUND(SUM(status_code_high_per_status_code_bucket) / NULLIF(COUNT(CASE WHEN attr_key = 'version' THEN 1 END), 0), 2) AS status_code_high_per_status_code_bucket_per_version,
    COUNT(CASE WHEN attr_key = 'region' THEN 1 END) AS region_count,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    GROUPING(CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM enriched
GROUP BY ROLLUP (CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END, seq_no, CASE WHEN status_code_high_per_status_code_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY max_status_code_high_per_status_code_bucket DESC;

WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        key,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE key IN ('version', 'trace')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        status_code_high,
        FIRST_VALUE(status_code_high, TRUE) OVER (PARTITION BY key ORDER BY status_code_high, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high,
        NTILE(4) OVER (PARTITION BY key ORDER BY has_fault, id, key) AS has_fault_bucket,
        CUME_DIST() OVER (PARTITION BY key ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_cume_dist,
        LAST_VALUE(has_fault) IGNORE NULLS OVER (ORDER BY has_fault, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_has_fault,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high DESC, id, key) AS seq_no
    FROM flattened_attrs
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        key,
        first_known_status_code_high,
        has_fault_bucket,
        ROUND(has_fault_bucket * 100, 2) AS has_fault_bucket_adjusted,
        COALESCE(key, 'NONE') AS key_filled
    FROM scored
    WHERE seq_no < 6
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN first_known_status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS first_known_status_code_high_tier,
        key,
        MAX(has_fault_bucket) FILTER (WHERE key = 'region') AS region_max_has_fault_bucket,
        APPROX_PERCENTILE(has_fault_bucket, 0.5) AS has_fault_bucket_p50,
        GROUPING_ID() AS grouping_id
    FROM filtered
    GROUP BY GROUPING SETS ((CASE WHEN first_known_status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END, key), (CASE WHEN first_known_status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT has_fault_bucket_p50, grouping_id
FROM grouped
ORDER BY grouping_id NULLS FIRST;

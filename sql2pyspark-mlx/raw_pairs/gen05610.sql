WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(k) AS k_length,
        UPPER(k) AS k_upper,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM flattened
    WHERE k IS NOT NULL
      AND k IN ('device', 'utm_source')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(k_length) OVER (PARTITION BY k ORDER BY k_length, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_k_length,
        NTILE(4) OVER (PARTITION BY k ORDER BY k_length, id, k) AS k_length_bucket,
        NTILE(5) OVER (ORDER BY k_length DESC, id, k) AS k_length_bucket_2,
        STDDEV_SAMP(k_length) OVER (PARTITION BY k) AS k_stddev_samp_k_length
    FROM valid
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE customer_id IN (19, 23, 29)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        k,
        v,
        k_upper,
        k_filled,
        cumulative_k_length,
        k_length_bucket_2,
        k_stddev_samp_k_length,
        k_length_bucket_2 / NULLIF(cumulative_k_length, 0) AS k_length_bucket_2_per_cumulative_k_length
    FROM kept_rows
    WHERE k IS NOT NULL
)
-- Multi-level aggregation
SELECT
    k,
    CASE WHEN k_stddev_samp_k_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS k_stddev_samp_k_length_tier,
    ROUND(SUM(k_stddev_samp_k_length) / NULLIF(COUNT(CASE WHEN k = 'utm_source' THEN 1 END), 0), 2) AS k_stddev_samp_k_length_per_utm_source,
    MAX(k_length_bucket_2) AS max_k_length_bucket_2,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM cleaned
GROUP BY k, CASE WHEN k_stddev_samp_k_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING max_k_length_bucket_2 > 0;

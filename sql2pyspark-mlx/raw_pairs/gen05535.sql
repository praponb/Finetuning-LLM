WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        k,
        attr_value,
        LENGTH(k) AS k_length,
        CASE WHEN k = 'utm_source' THEN 1 ELSE 0 END AS is_utm_source,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM exploded
    WHERE k IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) < '2025-07-06'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        attr_value,
        resolved_k,
        MIN(k_length) OVER w_running AS k_min_k_length,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY k_length DESC NULLS FIRST) AS k_length_dense_rank,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY k_length DESC) AS k_length_dense_rank_2
    FROM kept_rows
    WINDOW w_running AS (PARTITION BY k)
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(k_length_dense_rank, 0) AS k_length_dense_rank_filled
    FROM scored
    WHERE k_length_dense_rank_2 > 4
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN k_min_k_length >= 79 THEN 'HIGH' ELSE 'LOW' END AS k_min_k_length_tier,
        CASE WHEN k_length_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS k_length_dense_rank_tier,
        ROUND(AVG(k_length_dense_rank), 2) AS avg_k_length_dense_rank,
        ROUND(AVG(k_length_dense_rank_filled), 2) AS avg_k_length_dense_rank_filled,
        ROUND(AVG(k_length_dense_rank_2), 2) AS avg_k_length_dense_rank_2,
        ROUND(AVG(k_min_k_length), 2) AS avg_k_min_k_length,
        MIN(k_min_k_length) AS min_k_min_k_length,
        ROUND(AVG(k_length_dense_rank_2), 2) AS avg_k_length_dense_rank_2_2
    FROM filtered_carts
    GROUP BY CUBE (CASE WHEN k_min_k_length >= 79 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN k_length_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END)
    HAVING ROUND(AVG(k_length_dense_rank_2), 2) < 1000000
)
SELECT
    k_min_k_length_tier,
    k_length_dense_rank_tier,
    avg_k_length_dense_rank_2,
    avg_k_min_k_length,
    min_k_min_k_length
FROM stats;

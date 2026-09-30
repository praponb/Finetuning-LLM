WITH with_dims AS (
    -- Attach stores attributes
    SELECT e.*, b.sqft
    FROM inventory e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        sqft,
        ROUND(quantity * 1.15, 2) quantity_scaled
    FROM with_dims
    WHERE last_restock >= DATE '2023-02-23'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_sqft,
        AVG(sqft) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_sqft,
        COUNT(sqft) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_sqft,
        SUM(sqft) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_sqft,
        DENSE_RANK() OVER (ORDER BY sqft DESC NULLS LAST) sqft_dense_rank
    FROM valid
    WHERE quantity_scaled BETWEEN 38 AND 48
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, last3_avg_sqft - last3_count_sqft last3_avg_sqft_minus_last3_count_sqft
    FROM scored
    WHERE store_id IN (5, 6)
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft >= 16903 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    MAX(last3_max_sqft) AS max_last3_max_sqft,
    ROUND(AVG(last3_max_sqft), 2) AS avg_last3_max_sqft
FROM valid_inventory
GROUP BY CASE WHEN sqft >= 16903 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_last3_max_sqft DESC NULLS FIRST;

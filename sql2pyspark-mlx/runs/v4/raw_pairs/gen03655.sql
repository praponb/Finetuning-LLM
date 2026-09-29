WITH lines AS (
    SELECT
        id,
        created_at,
        elem.product_id
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS elem
),
scored AS (
    SELECT
        *,
        RANK() OVER w_grp AS product_id_rank,
        AVG(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_avg_1h
    FROM lines
    WINDOW w_grp AS (ORDER BY product_id NULLS FIRST)
),
cleaned AS (
    SELECT *, CASE WHEN product_id_rank >= 9 THEN 'High' WHEN product_id_rank >= 4 THEN 'Medium' ELSE 'Low' END AS product_id_rank_band
    FROM scored
    WHERE product_id BETWEEN 6 AND 9
),
filtered AS (
    SELECT
        id,
        created_at,
        product_id_rank,
        product_id_avg_1h,
        product_id_rank_band,
        ROUND(product_id, 2) AS product_id_rounded,
        GREATEST(product_id, product_id_avg_1h) AS greatest_product_id_product_id_avg_1h
    FROM cleaned
    WHERE created_at >= TIMESTAMP '2025-03-01 20:00:00'
)
SELECT
    created_at,
    product_id_rank,
    product_id_avg_1h,
    product_id_rank_band,
    product_id_rounded,
    greatest_product_id_product_id_avg_1h
FROM filtered
ORDER BY greatest_product_id_product_id_avg_1h, product_id_rounded DESC

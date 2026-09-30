WITH lines AS (
    SELECT
        base.id,
        base.customer_id,
        pos + 1 AS position,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts base
    LATERAL VIEW POSEXPLODE(base.items) t AS pos, item
),
kept_keys AS (
    SELECT id
    FROM lines
    INTERSECT
    SELECT id
    FROM carts
    WHERE SIZE(coupon_codes) >= 1
),
kept_rows AS (
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
with_dims AS (
    SELECT
        src.*,
        c.segment,
        c.name
    FROM kept_rows src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.segment IN ('Corporate', 'Consumer')
),
metrics AS (
    SELECT
        id,
        product_id,
        qty,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY product_id NULLS LAST) product_id_dense_rank,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY qty) qty_dense_rank
    FROM with_dims
),
filtered AS (
    SELECT *, COALESCE(product_id_dense_rank, 0) product_id_dense_rank_filled
    FROM metrics
    WHERE product_id_dense_rank BETWEEN 4 AND 7
)
SELECT
    product_id,
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    MIN(product_id) AS min_product_id,
    MAX(product_id_dense_rank_filled) AS max_product_id_dense_rank_filled,
    ROUND(AVG(product_id), 2) AS avg_product_id,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(product_id) AS max_product_id
FROM filtered
GROUP BY product_id, CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END
HAVING min_product_id < 1000000
ORDER BY distinct_ids DESC NULLS LAST;

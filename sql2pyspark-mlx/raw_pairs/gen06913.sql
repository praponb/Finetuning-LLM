WITH enriched AS (
    SELECT
        *,
        CORR(price, cost) OVER (PARTITION BY brand) AS corr_price_cost,
        RANK() OVER (PARTITION BY brand, category ORDER BY stock_qty DESC) AS stock_qty_rank,
        NTILE(3) OVER (PARTITION BY category ORDER BY price, id) AS price_bucket
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_latest AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.cost,
        s.launch_date,
        s.supplier_id,
        s.stock_qty_rank,
        s.price_bucket,
        latest.unit_price
    FROM enriched s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.launch_date,
        s.supplier_id,
        s.stock_qty_rank,
        s.price_bucket,
        s.unit_price,
        top1.units,
        top1.sale_date
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.units, c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.launch_date,
        a.supplier_id,
        a.stock_qty_rank,
        a.price_bucket,
        a.sale_date,
        p.city
    FROM latest_match a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
)
SELECT
    CASE WHEN stock_qty_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_rank_tier,
    COUNT_IF(category = 'Home') AS home_events,
    ROUND(AVG(price_bucket), 2) AS avg_price_bucket,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM joined
GROUP BY CASE WHEN stock_qty_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING num_cities >= 1
ORDER BY num_cities NULLS FIRST

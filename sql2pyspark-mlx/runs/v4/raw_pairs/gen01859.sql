WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER (ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior,
        MAX(price) OVER (PARTITION BY brand) AS brand_max_price,
        LAG(price, 1) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_price,
        CORR(stock_qty, price) OVER (PARTITION BY category, brand) AS corr_stock_qty_price
    FROM products
)
SELECT
    s.id,
    s.product_name,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    s.cost_avg_6_prior,
    CASE WHEN brand RLIKE '^(Acme|Umbrella)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS brand_zone,
    (SELECT MAX(t2.stock_qty) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS max_product_stock_qty
FROM enriched s
ORDER BY stock_qty, price DESC

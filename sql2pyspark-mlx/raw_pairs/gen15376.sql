WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.supplier_name,
        b.country,
        b.city
    FROM products e
    INNER JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.rating BETWEEN 1.9 AND 2.2
)
-- Grouped metrics
SELECT
    supplier_id,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN country = 'JP' THEN 1 END), 0), 2) AS stock_qty_per_jp,
    MAX(price) AS max_price,
    CORR(cost, stock_qty) AS corr_cost_stock_qty
FROM joined
GROUP BY supplier_id
HAVING stock_qty_per_jp > 0

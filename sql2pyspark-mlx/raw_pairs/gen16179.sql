SELECT
    p.id,
    p.cost,
    t.unit_price_range,
    t.revenue
FROM products p
LEFT JOIN (SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY cost DESC

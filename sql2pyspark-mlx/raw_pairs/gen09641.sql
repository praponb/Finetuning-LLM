WITH order_items_by_product AS (
    SELECT product_id, SUM(quantity * unit_price) revenue, MAX(unit_price) - MIN(unit_price) unit_price_range
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    oibp.revenue,
    oibp.unit_price_range
FROM products p
LEFT JOIN order_items_by_product oibp ON p.id = oibp.product_id
WHERE oibp.revenue > 73
ORDER BY 3

WITH order_items_by_product AS (
    SELECT product_id, SUM(unit_price * quantity) revenue
    FROM order_items
    WHERE unit_price <= 390
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    p.launch_date,
    oibp.revenue
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id;

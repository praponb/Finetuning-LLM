WITH order_items_by_product AS (
    SELECT product_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT p.price, oibp.revenue
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id
WHERE oibp.revenue > 40;

WITH order_items_by_product AS (
    SELECT product_id, COUNT(unit_price) AS unit_price_count
    FROM order_items
    WHERE product_id = 27
    GROUP BY product_id
)
SELECT p.category, oibp.unit_price_count
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id
ORDER BY category DESC, unit_price_count;

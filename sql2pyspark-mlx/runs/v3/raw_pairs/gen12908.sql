SELECT
    order_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(unit_price * quantity) AS revenue,
    COUNT(*) AS order_item_count
FROM order_items
WHERE product_id IS NOT NULL OR quantity < 6
GROUP BY order_id
HAVING COUNT(*) < 2;

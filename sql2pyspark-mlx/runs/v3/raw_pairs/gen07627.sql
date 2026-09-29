WITH ranked_order_items AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS rnk
    FROM order_items
)
SELECT id, product_id, unit_price
FROM ranked_order_items
WHERE rnk <= 2
ORDER BY product_id DESC;

SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products
FROM order_items
GROUP BY product_id;

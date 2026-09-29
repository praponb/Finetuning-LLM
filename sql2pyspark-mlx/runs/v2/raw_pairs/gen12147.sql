SELECT product_id, COUNT(DISTINCT quantity) AS unique_products FROM order_items GROUP BY 1;

SELECT store_id, COUNT(DISTINCT quantity) AS unique_products FROM inventory WHERE product_id IN (2, 3) GROUP BY store_id ORDER BY store_id DESC LIMIT 5;

SELECT product_id, unique_products FROM (SELECT product_id, COUNT(DISTINCT quantity) AS unique_products FROM inventory GROUP BY product_id) AS s WHERE unique_products < 8;

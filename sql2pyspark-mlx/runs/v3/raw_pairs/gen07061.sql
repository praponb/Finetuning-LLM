SELECT store_id, COUNT(DISTINCT quantity) AS unique_products FROM inventory GROUP BY store_id ORDER BY store_id DESC

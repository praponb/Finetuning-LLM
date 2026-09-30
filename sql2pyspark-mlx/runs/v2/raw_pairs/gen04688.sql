SELECT QUARTER(last_restock) AS last_restock_quarter, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE product_id = 10
GROUP BY QUARTER(last_restock)
HAVING COUNT(DISTINCT quantity) > 2;

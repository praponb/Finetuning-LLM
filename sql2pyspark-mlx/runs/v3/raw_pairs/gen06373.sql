SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, MIN(quantity) AS min_quantity
FROM inventory
WHERE product_id IN (4, 6, 9)
GROUP BY last_restock_month;

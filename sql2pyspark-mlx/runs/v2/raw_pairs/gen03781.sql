SELECT product_id, MONTH(last_restock) AS last_restock_month, MAX(last_restock) AS last_last_restock
FROM inventory
WHERE quantity < 154 AND store_id IN (2, 6)
GROUP BY product_id, last_restock_month
ORDER BY last_last_restock DESC, product_id

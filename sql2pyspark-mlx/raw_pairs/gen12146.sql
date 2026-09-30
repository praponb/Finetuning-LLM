SELECT *, DATE_FORMAT(last_restock, 'MMM yyyy') last_restock_label FROM inventory WHERE store_id IN (1, 3, 5, 6) AND YEAR(last_restock) = 2025 ORDER BY last_restock

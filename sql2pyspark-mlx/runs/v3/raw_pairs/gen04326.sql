SELECT *, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE last_restock > '2023-05-01' AND (store_id = 6 OR store_id IN (1, 6)) ORDER BY last_restock_month_end, last_restock

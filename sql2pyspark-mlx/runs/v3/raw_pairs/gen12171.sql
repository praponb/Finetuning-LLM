SELECT product_id, store_id, quantity, DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label FROM inventory WHERE last_restock BETWEEN '2024-03-01' AND '2024-03-30' AND store_id = 6;

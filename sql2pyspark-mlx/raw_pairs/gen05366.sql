SELECT last_restock AS event_date FROM inventory WHERE last_restock BETWEEN '2024-03-01' AND '2025-06-01' UNION SELECT sale_date AS event_date FROM store_sales

SELECT id, store_id, DATEDIFF(CURRENT_DATE(), last_restock) / 365 AS years_since_last_restock FROM inventory WHERE last_restock BETWEEN '2024-01-21' AND '2024-03-01';

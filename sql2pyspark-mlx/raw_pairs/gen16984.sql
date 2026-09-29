SELECT sale_date AS event_date FROM store_sales UNION ALL SELECT last_restock AS event_date FROM inventory WHERE quantity > 72 ORDER BY event_date;

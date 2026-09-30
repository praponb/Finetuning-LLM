SELECT opened_date FROM stores WHERE opened_date IS NOT NULL UNION ALL SELECT order_date FROM orders WHERE order_date IS NOT NULL ORDER BY opened_date DESC

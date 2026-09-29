SELECT order_date, SUM(CASE WHEN status = 'RETURNED' THEN amount ELSE 0 END) AS total_returned FROM orders GROUP BY order_date;

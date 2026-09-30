SELECT * FROM (SELECT status, SUM(CASE WHEN status = 'SHIPPED' THEN amount ELSE 0 END) AS shipped_amount, AVG(amount) AS avg_amount FROM orders GROUP BY status) AS sub WHERE shipped_amount < 55;

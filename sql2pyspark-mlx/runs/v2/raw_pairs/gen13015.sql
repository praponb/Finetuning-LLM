SELECT SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_count, MAX(discount) AS max_discount FROM orders;

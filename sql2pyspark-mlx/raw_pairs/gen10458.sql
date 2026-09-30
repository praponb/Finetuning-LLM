SELECT * FROM (SELECT store_id, status, discount FROM orders) src PIVOT (AVG(discount) FOR status IN ('RETURNED', 'PENDING', 'CANCELLED'));

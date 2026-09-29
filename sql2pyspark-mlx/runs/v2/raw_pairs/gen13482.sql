SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (SUM(discount) FOR status IN ('SHIPPED', 'PENDING', 'CANCELLED', 'RETURNED'));

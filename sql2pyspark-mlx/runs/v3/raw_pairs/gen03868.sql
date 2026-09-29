SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (AVG(discount) FOR status IN ('CANCELLED', 'RETURNED', 'SHIPPED', 'COMPLETED')) ORDER BY channel;

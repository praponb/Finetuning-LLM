SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (MAX(discount) FOR status IN ('CANCELLED', 'SHIPPED', 'COMPLETED'))

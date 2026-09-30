SELECT * FROM (SELECT channel, status, amount FROM orders WHERE channel IN ('store', 'mobile'))
PIVOT (COUNT(amount) FOR status IN ('COMPLETED', 'PENDING', 'SHIPPED'))

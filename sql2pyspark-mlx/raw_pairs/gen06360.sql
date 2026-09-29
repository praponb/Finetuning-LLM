SELECT * FROM (SELECT channel, status, shipping_fee FROM orders) PIVOT (COUNT(shipping_fee) FOR status IN ('SHIPPED', 'PENDING', 'CANCELLED', 'COMPLETED'));

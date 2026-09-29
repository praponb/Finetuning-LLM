SELECT * FROM (SELECT customer_id, status, shipping_fee FROM orders) PIVOT (COUNT(shipping_fee) FOR status IN ('SHIPPED', 'COMPLETED'));

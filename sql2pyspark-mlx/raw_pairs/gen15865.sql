SELECT * FROM (SELECT id, customer_id, amount, discount, shipping_fee FROM orders) src UNPIVOT (metric_value FOR metric_name IN (amount, discount, shipping_fee));

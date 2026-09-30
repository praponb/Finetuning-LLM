SELECT * FROM (SELECT id, store_id, discount, amount, shipping_fee FROM orders) src UNPIVOT INCLUDE NULLS (metric_value FOR metric IN (discount, amount, shipping_fee));

SELECT id, status, measure, metric_value FROM (SELECT id, status, discount, shipping_fee FROM orders) src UNPIVOT (metric_value FOR measure IN (discount, shipping_fee));

SELECT * FROM (SELECT id, customer_id, amount, shipping_fee, discount FROM orders) src UNPIVOT (reading FOR kpi IN (amount, shipping_fee, discount));

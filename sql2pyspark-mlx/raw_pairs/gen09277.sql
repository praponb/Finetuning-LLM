SELECT id, amount, channel, CASE WHEN shipping_fee >= 44 THEN 'High' WHEN shipping_fee >= 10 THEN 'Medium' ELSE 'Low' END AS shipping_fee_level FROM orders;

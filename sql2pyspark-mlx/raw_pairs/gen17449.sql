SELECT * FROM (SELECT status, channel, shipping_fee FROM orders WHERE order_date < '2025-07-31') PIVOT (COUNT(shipping_fee) FOR channel IN ('web', 'partner', 'mobile')) ORDER BY status;

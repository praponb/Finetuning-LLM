SELECT status, channel, customer_id, discount / NULLIF(shipping_fee, 0) AS discount_per_shipping_fee FROM orders WHERE (amount < 1060 AND order_date >= '2023-09-01') OR customer_id = 8;

SELECT shipping_fee FROM orders WHERE customer_id IN (3, 10) AND (store_id IS NOT NULL OR order_date > '2025-07-29') ORDER BY shipping_fee DESC;

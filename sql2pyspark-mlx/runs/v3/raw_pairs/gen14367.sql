SELECT id, order_date, store_id, status, DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date, CEIL(shipping_fee) AS shipping_fee_whole FROM orders WHERE status = 'PENDING';

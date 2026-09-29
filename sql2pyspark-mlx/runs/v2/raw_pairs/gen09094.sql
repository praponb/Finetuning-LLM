SELECT id, order_date, store_id, ROUND(shipping_fee * 100, 2) shipping_fee_scaled FROM orders WHERE amount < 1500;

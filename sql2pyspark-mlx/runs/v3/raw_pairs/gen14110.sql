SELECT store_id, order_date, shipping_fee, LOWER(status) AS status_lower FROM orders WHERE amount BETWEEN 710 AND 1180

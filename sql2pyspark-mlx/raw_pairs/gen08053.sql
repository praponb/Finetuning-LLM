SELECT o.shipping_fee, o.store_id, o.order_date, c.signup_date, CONCAT_WS(',', c.name, c.city) AS name_city_key FROM orders o INNER JOIN customers c ON o.customer_id = c.id;

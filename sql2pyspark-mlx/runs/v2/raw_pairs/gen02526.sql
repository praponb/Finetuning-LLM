SELECT o.order_date, o.store_id, c.name, c.email, c.signup_date, CAST(o.shipping_fee AS INT) AS shipping_fee_int FROM orders o LEFT JOIN customers c ON c.id = o.customer_id ORDER BY store_id DESC

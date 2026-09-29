SELECT o.id, o.channel, c.country, s.store_name, CAST(o.amount AS INT) AS amount_int FROM orders o LEFT OUTER JOIN customers c ON o.customer_id = c.id LEFT OUTER JOIN stores s ON o.store_id = s.id;

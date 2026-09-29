SELECT * FROM (SELECT id, channel, order_date, shipping_fee, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id) AS rn FROM orders) AS r WHERE rn <= 3 ORDER BY rn DESC;

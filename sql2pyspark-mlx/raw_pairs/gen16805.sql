SELECT s.opened_date, o.shipping_fee, o.amount, o.customer_id, LOWER(s.city) AS city_lower FROM stores s INNER JOIN orders o ON s.id = o.store_id WHERE o.channel NOT IN ('web', 'store', 'partner');

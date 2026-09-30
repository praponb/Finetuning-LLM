WITH ranked_orders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS rn FROM orders) SELECT * FROM ranked_orders WHERE rn <= 5

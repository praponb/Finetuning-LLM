WITH ranked AS (SELECT * FROM promotions QUALIFY RANK() OVER (PARTITION BY promo_code, channel ORDER BY ends_on) = 1) SELECT id FROM ranked

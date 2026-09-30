WITH ranked AS (SELECT * FROM promotions QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) = 1) SELECT id, channel, starts_on, ends_on FROM ranked ORDER BY id DESC

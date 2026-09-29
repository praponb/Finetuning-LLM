SELECT * FROM (SELECT channel, promo_code, discount_pct FROM promotions) src PIVOT (SUM(discount_pct) AS amount FOR promo_code IN ('BUNDLE', 'VIP'))

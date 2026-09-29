SELECT * FROM (SELECT channel, promo_code, discount_pct FROM promotions) src PIVOT (SUM(discount_pct) AS total, COUNT(1) AS cnt FOR promo_code IN ('SPRING10' AS spring10, 'VIP' AS vip));

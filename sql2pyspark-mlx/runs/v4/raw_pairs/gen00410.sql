SELECT * FROM (SELECT promo_code, channel, discount_pct FROM promotions) src PIVOT (AVG(discount_pct) FOR channel IN ('store', 'web', 'partner'));

SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id) AS rank_in_group FROM promotions) x WHERE rank_in_group = 1

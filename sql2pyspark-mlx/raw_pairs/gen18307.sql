SELECT id, ends_on FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id ASC) AS dedup_rank FROM promotions) x WHERE dedup_rank = 1

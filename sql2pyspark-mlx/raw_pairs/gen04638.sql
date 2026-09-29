SELECT
    id,
    channel,
    discount_pct,
    AVG(discount_pct) OVER (PARTITION BY DATE_TRUNC('MONTH', ends_on), promo_code ORDER BY starts_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
    LEAD(discount_pct, 2) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS next_discount_pct,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS row_seq,
    PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) AS discount_pct_percent_rank
FROM promotions
WHERE ends_on > '2025-02-01'

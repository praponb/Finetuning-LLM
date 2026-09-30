SELECT
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_pct_avg_3,
    SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
    PERCENT_RANK() OVER w_grp AS discount_pct_percent_rank
FROM promotions
WINDOW w_grp AS (PARTITION BY YEAR(ends_on), channel ORDER BY discount_pct)

SELECT
    promo_code,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_per_welcome5,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(discount_pct) AS avg_discount_pct
FROM promotions
GROUP BY promo_code
HAVING distinct_ids > 1
ORDER BY promo_code DESC NULLS FIRST

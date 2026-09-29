SELECT
    promo_code,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    SUM(CASE WHEN channel = 'partner' THEN discount_pct ELSE 0 END) AS partner_discount_pct
FROM promotions
GROUP BY promo_code;

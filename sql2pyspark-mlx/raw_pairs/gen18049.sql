SELECT
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS discount_pct_per_store,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM promotions
GROUP BY CUBE (DATE_TRUNC('WEEK', starts_on));

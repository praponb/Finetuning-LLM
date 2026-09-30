SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        discount_pct
    FROM promotions
) src
PIVOT (
    SUM(discount_pct) AS total
    FOR channel IN ('web' AS web, 'store' AS store)
);

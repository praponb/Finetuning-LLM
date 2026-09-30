SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM promotions
) src
PIVOT (
    SUM(discount_pct)
    FOR promo_code IN ('WELCOME5', 'VIP')
);

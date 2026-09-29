SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        discount_pct
    FROM promotions
) src
PIVOT (
    MAX(discount_pct) AS total
    FOR channel IN ('mobile', 'partner')
);

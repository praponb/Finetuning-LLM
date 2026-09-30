SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM promotions
) src
PIVOT (
    AVG(discount_pct) AS value_sum
    FOR promo_code IN ('BUNDLE' AS bundle, 'WELCOME5' AS welcome5)
)

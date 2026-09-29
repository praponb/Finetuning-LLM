WITH enriched AS (
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share
    FROM promotions
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct_share,
    CASE WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS promo_code_zone
FROM enriched
ORDER BY discount_pct_share DESC

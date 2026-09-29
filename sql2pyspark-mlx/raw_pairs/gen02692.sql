WITH metrics AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        LEAD(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS event_seq
    FROM promotions
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    next_discount_pct,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS channel_zone
FROM metrics
ORDER BY starts_on NULLS LAST

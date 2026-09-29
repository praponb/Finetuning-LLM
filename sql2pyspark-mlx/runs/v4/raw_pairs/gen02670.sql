WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    id,
    channel,
    starts_on,
    promo_id,
    promotion_discount_pct,
    CASE WHEN channel RLIKE '^(mobile|partner)$' THEN 'EXTENDED' WHEN channel RLIKE '^(web|store)$' THEN 'CORE' ELSE 'PRIORITY' END AS channel_zone
FROM attributed
ORDER BY promo_id DESC NULLS FIRST, channel

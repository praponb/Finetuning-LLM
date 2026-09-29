WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(promotion_discount_pct * 1.1, 2) AS promotion_discount_pct_scaled,
        COALESCE(channel, 'unassigned') AS channel_filled
    FROM attributed
    WHERE starts_on > DATE '2023-07-09'
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM filtered e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM attributed_promotions s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 30 DAYS
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    CASE WHEN channel RLIKE '^(partner|mobile)$' THEN 'CORE' WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS channel_zone
FROM with_window
ORDER BY promotion_promo_code DESC NULLS LAST, promo_id ASC

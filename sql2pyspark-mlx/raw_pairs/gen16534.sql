WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(channel, 'Unknown') AS channel_filled
    FROM promotions
    WHERE starts_on < '2024-09-01'
      AND starts_on > DATE '2023-06-02'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid src
    JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        channel_filled,
        promo_id,
        promotion_discount_pct,
        LEAD(discount_pct, 2) OVER (PARTITION BY channel ORDER BY promo_id, id, promo_id) AS next_discount_pct,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promo_code, channel), 4) AS promo_id_share,
        AVG(promotion_discount_pct) OVER (PARTITION BY DATE_TRUNC('week', starts_on) ORDER BY promo_id, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS promotion_discount_pct_avg_6_prior
    FROM with_window
    WHERE YEAR(ends_on) = 2025
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        next_discount_pct,
        promo_id_share,
        promotion_discount_pct_avg_6_prior,
        CASE WHEN channel IN ('mobile', 'partner') THEN 'Core' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM enriched
    WHERE channel IS DISTINCT FROM 'store'
)
SELECT
    id,
    ends_on,
    discount_pct,
    promo_id,
    promotion_discount_pct,
    next_discount_pct,
    resolved_channel
FROM filtered

WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM promotions
    WHERE channel IS DISTINCT FROM 'web'
      AND channel IS NOT NULL
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 10)
),
latest AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        channel_clean,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, promo_code ORDER BY discount_pct, id, promo_id) AS rank_in_group
        FROM matched
    ) t
    WHERE rank_in_group = 1
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM latest src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '30' DAY
),
summary AS (
    SELECT
        DATE_TRUNC('YEAR', starts_on) AS year_start,
        CASE WHEN promo_id_2 >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_2_tier,
        STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
        COUNT_IF(channel = 'partner') AS partner_events,
        RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', starts_on) ORDER BY STDDEV_SAMP(discount_pct) DESC) AS discount_pct_stddev_rank
    FROM with_window
    GROUP BY year_start, promo_id_2_tier
)
SELECT
    year_start,
    promo_id_2_tier,
    partner_events,
    discount_pct_stddev_rank
FROM summary
ORDER BY partner_events DESC

WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ADD_MONTHS(ends_on, 1) AS ends_on_plus_months
    FROM promotions
    WHERE ends_on > DATE '2023-05-23'
      AND channel IS NOT DISTINCT FROM 'partner'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '7' DAY
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM attributed s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ends_on_plus_months,
        promo_id,
        promotion_promo_code,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        promo_id_3,
        promotion_promo_code_3,
        CEIL(promotion_discount_pct) AS promotion_discount_pct_whole,
        LOWER(promo_code) AS promo_code_lower
    FROM matched_promotions
    WHERE ends_on > DATE '2023-05-26'
      AND MONTH(ends_on_plus_months) IN (1, 2, 9)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promo_id_2,
        promo_id_3,
        promotion_promo_code_3,
        promo_code_lower,
        LAST_VALUE(promotion_promo_code_3) IGNORE NULLS OVER w_grp AS last_known_promotion_promo_code_3,
        ROUND(promotion_discount_pct_2 / SUM(promotion_discount_pct_2) OVER (PARTITION BY promo_code), 4) AS promotion_discount_pct_2_share,
        AVG(promotion_discount_pct_2) OVER (PARTITION BY promo_code ORDER BY promo_id_3, id, promo_id, promo_id_2, promo_id_3 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS promotion_discount_pct_2_avg_3_prior
    FROM filtered
    WHERE ends_on >= '2025-03-07'
    WINDOW w_grp AS (PARTITION BY promotion_promo_code, promo_code ORDER BY promo_id, id, promo_id, promo_id_2, promo_id_3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            promo_code,
            promotion_promo_code_3,
            promotion_discount_pct_2_avg_3_prior
        FROM with_windows
    ) src
    PIVOT (
        SUM(promotion_discount_pct_2_avg_3_prior)
        FOR promotion_promo_code_3 IN ('WELCOME5', 'SPRING10')
    )
)
SELECT
    promo_code,
    WELCOME5,
    CASE WHEN promo_code RLIKE '^(BUNDLE|VIP)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS promo_code_zone
FROM pivoted;

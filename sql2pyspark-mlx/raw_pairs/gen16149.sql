WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        ADD_MONTHS(starts_on, 12) AS starts_on_plus_months,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM promotions
    WHERE ends_on > DATE '2023-10-12'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on_plus_months >= pr.starts_on
       AND a.starts_on_plus_months < pr.ends_on + INTERVAL 7 DAYS
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        is_bundle,
        starts_on_plus_months,
        channel_clean,
        promo_id,
        promo_code,
        promotion_discount_pct,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle_2,
        LOWER(promo_code) AS promo_code_lower
    FROM matched
    WHERE ends_on >= DATE '2023-08-07'
      AND promo_code IS DISTINCT FROM 'BUNDLE'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM cleaned a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 3)
),
cleaned_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        promo_id,
        promo_code,
        promo_code_lower,
        promo_id_2,
        promotion_promo_code,
        promotion_discount_pct_2,
        YEAR(starts_on_plus_months) AS starts_on_plus_months_year
    FROM with_window
    WHERE channel IN ('partner', 'web', 'mobile')
      AND channel IS NOT DISTINCT FROM 'web'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_id DESC NULLS FIRST, id ASC, promo_id ASC, promo_id_2 ASC) AS rn
    FROM cleaned_promotions
    QUALIFY rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        promo_id_2,
        promotion_promo_code,
        promotion_discount_pct_2,
        discount_pct
    FROM deduped
) src
UNPIVOT (
    reading FOR kpi IN (promotion_discount_pct_2, discount_pct)
)

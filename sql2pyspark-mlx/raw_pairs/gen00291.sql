WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on
    FROM promotions
    WHERE channel IS NOT NULL
      AND MONTH(starts_on) IN (8, 9, 12)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY days_ends_on_to_starts_on ASC NULLS LAST, id ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(days_ends_on_to_starts_on, promo_id) AS greatest_days_ends_on_to_starts_on_promo_id,
        FLOOR(days_ends_on_to_starts_on) AS days_ends_on_to_starts_on_whole,
        COALESCE(promotion_promo_code, 'NONE') AS resolved_promotion_promo_code
    FROM with_window
    WHERE ends_on >= TO_DATE('2023-10-30')
      AND promotion_promo_code IS NOT NULL
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, channel ORDER BY promotion_discount_pct DESC NULLS FIRST, id ASC, promo_id ASC) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    promo_id,
    promotion_promo_code,
    measure,
    val
FROM (
    SELECT
        id,
        promo_id,
        promotion_promo_code,
        greatest_days_ends_on_to_starts_on_promo_id,
        discount_pct,
        promotion_discount_pct
    FROM latest_promotions
) src
UNPIVOT (
    val FOR measure IN (greatest_days_ends_on_to_starts_on_promo_id, discount_pct, promotion_discount_pct)
)

WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY ends_on ASC) AS rn
        FROM promotions
    ) x
    WHERE rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT promo_code
    FROM ranked
    INTERSECT ALL
    SELECT promo_code
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.promo_code = k.promo_code
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM verified src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        COALESCE(promotion_promo_code, 'DIRECT') AS promotion_promo_code_clean
    FROM with_window
    WHERE promotion_promo_code = 'BUNDLE'
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM cleaned e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 7 DAYS
),
with_window_promotions_2 AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM with_window_promotions src
    JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        starts_on,
        promotion_promo_code,
        is_bundle,
        promotion_promo_code_clean,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        promotion_discount_pct_3,
        discount_pct + promo_id AS discount_pct_plus_promo_id,
        COALESCE(promotion_promo_code_2, 'NONE') AS resolved_promotion_promo_code_2
    FROM with_window_promotions_2
    WHERE starts_on >= TO_DATE('2023-05-12')
      AND discount_pct <= 0.31
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(promotion_discount_pct_3 / SUM(promotion_discount_pct_3) OVER (PARTITION BY promotion_promo_code), 4) AS promotion_discount_pct_3_share,
    ROUND(promotion_discount_pct_3 / SUM(promotion_discount_pct_3) OVER (PARTITION BY promotion_promo_code_2, promotion_promo_code), 4) AS promotion_discount_pct_3_share_2
FROM valid

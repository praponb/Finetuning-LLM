WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked e
    JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched src
    JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(promo_code) AS promo_code_upper
    FROM matched_promotions
    WHERE ends_on >= TO_DATE('2023-05-23')
      AND promotion_promo_code = 'BUNDLE'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promo_code_upper
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code_2 ORDER BY promo_id_2 DESC, id, promo_id, promo_id_2) = 1
),
ranked_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id, promo_id, promo_id_2) AS latest_rank
        FROM latest
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promo_code_upper,
        DATEDIFF(CURRENT_DATE(), ends_on) / 7 AS weeks_since_ends_on
    FROM ranked_promotions
    WHERE channel IS DISTINCT FROM 'web'
      AND channel IS NOT NULL
)
SELECT
    id,
    promo_code,
    ends_on,
    promo_id,
    promotion_discount_pct,
    promo_id_2,
    promo_code_upper
FROM base
ORDER BY promo_id_2 DESC;

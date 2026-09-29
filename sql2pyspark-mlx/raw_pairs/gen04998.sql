WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(promo_code, 1, 2) promo_code_prefix,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM promotions
    WHERE starts_on > TO_DATE('2023-04-30')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 14 DAYS
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_prefix,
        channel_filled,
        promo_id,
        promotion_discount_pct,
        GREATEST(promo_id, promotion_discount_pct) greatest_promo_id_promotion_discount_pct,
        ROUND(promo_id, 2) promo_id_rounded
    FROM with_window
    WHERE ends_on > TO_DATE('2023-06-15')
      AND promotion_promo_code IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        starts_on,
        promo_code_prefix,
        promo_id,
        promotion_discount_pct,
        promo_id_rounded
    FROM base
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) = 1
)
SELECT
    id,
    channel,
    promo_id,
    promotion_discount_pct,
    promo_id_rounded
FROM deduped
ORDER BY channel DESC NULLS LAST;

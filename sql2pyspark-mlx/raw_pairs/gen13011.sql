WITH base AS (
    SELECT
        *,
        DATEDIFF(ends_on, starts_on) / 7 weeks_starts_on_to_ends_on,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM promotions
    WHERE ends_on > TO_DATE('2023-12-12')
      AND channel IS NOT NULL
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) = 1
),
valid AS (
    SELECT
        *,
        UPPER(promo_code) promo_code_upper,
        ROUND(discount_pct, 1) discount_pct_rounded
    FROM ranked
    WHERE channel IS DISTINCT FROM 'store'
),
matched_keys AS (
    SELECT channel, promo_code
    FROM valid
    EXCEPT DISTINCT
    SELECT channel, promo_code
    FROM promotions
    WHERE channel IN ('mobile', 'store')
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM kept_rows src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    weeks_starts_on_to_ends_on,
    resolved_channel,
    promo_code_upper,
    discount_pct_rounded
FROM attributed
ORDER BY id DESC, ends_on

WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        ROUND(discount_pct, 2) AS discount_pct_rounded,
        ROUND(discount_pct * 0.9, 2) AS adjusted_discount_pct,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IN ('mobile', 'store', 'web')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(adjusted_discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id) AS next_adjusted_discount_pct,
        LEAD(discount_pct_rounded) OVER (PARTITION BY channel ORDER BY starts_on, id) AS next_discount_pct_rounded,
        LAG(discount_pct_rounded) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct_rounded,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS row_seq
    FROM filtered
    WHERE ends_on BETWEEN '2023-03-07' AND '2023-10-07'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY prev_discount_pct_rounded NULLS FIRST) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
latest_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_rounded DESC, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        next_discount_pct_rounded
    FROM latest_promotions
) src
PIVOT (
    AVG(next_discount_pct_rounded) AS value_sum
    FOR channel IN ('mobile', 'partner', 'web')
);

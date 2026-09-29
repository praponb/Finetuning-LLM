WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        MONTH(starts_on) AS starts_on_month,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE ends_on > '2023-04-01'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount_pct AS INT) AS discount_pct_int,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM valid
    WHERE channel IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        ends_on,
        starts_on_month,
        channel_filled,
        LAG(starts_on_month) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS prev_starts_on_month,
        LEAD(starts_on_month) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_starts_on_month,
        LEAD(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS next_discount_pct,
        LAG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS row_seq
    FROM latest
    WHERE discount_pct_int > 68
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(starts_on_month) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id) AS next_starts_on_month_2,
        LAG(prev_discount_pct) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id) AS prev_prev_discount_pct,
        LEAD(prev_discount_pct) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id) AS next_prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id) AS seq_no
    FROM enriched
)
SELECT
    id,
    ends_on,
    starts_on_month,
    prev_starts_on_month,
    next_discount_pct,
    prev_discount_pct,
    row_seq,
    next_starts_on_month_2,
    prev_prev_discount_pct
FROM metrics
ORDER BY next_starts_on_month_2 DESC NULLS LAST

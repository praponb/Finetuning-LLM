WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id) = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        channel_filled
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY ends_on DESC) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        channel_filled,
        LAG(discount_pct) OVER w_part AS prev_discount_pct,
        LEAD(discount_pct) OVER w_part AS next_discount_pct,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM latest
    WHERE channel <> 'store'
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY discount_pct, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', ends_on) AS year_start,
        DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
        SUM(CASE WHEN channel = 'web' THEN next_discount_pct ELSE 0 END) AS web_next_discount_pct,
        APPROX_PERCENTILE(seq_no, 0.9) AS seq_no_p90
    FROM enriched
    GROUP BY 1, 2
)
SELECT
    year_start,
    quarter_start,
    web_next_discount_pct
FROM totals
ORDER BY web_next_discount_pct NULLS LAST, year_start;

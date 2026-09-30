WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN promo_code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5
    FROM promotions
    WHERE ends_on > TO_DATE('2023-10-09')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(DATEDIFF(ends_on, starts_on) / 30, 1) AS months_starts_on_to_ends_on
    FROM cleaned
    WHERE ends_on <= '2023-12-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY is_welcome5 NULLS LAST, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
valid_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        is_welcome5,
        months_starts_on_to_ends_on,
        latest_rank,
        DATE_TRUNC('YEAR', ends_on) AS ends_on_period
    FROM deduped
    WHERE ends_on >= DATE '2023-07-29'
      AND months_starts_on_to_ends_on <= 27
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_welcome5 >= 72 THEN 'LARGE' ELSE 'SMALL' END AS is_welcome5_tier,
        CASE WHEN months_starts_on_to_ends_on >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS months_starts_on_to_ends_on_tier,
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        MAX(ends_on_period) AS last_ends_on_period,
        MAX(is_welcome5) FILTER (WHERE promo_code = 'SPRING10') AS spring10_max_is_welcome5,
        MIN(ends_on_period) AS first_seen,
        GROUPING(CASE WHEN is_welcome5 >= 72 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
    FROM latest
    GROUP BY GROUPING SETS ((CASE WHEN is_welcome5 >= 72 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN months_starts_on_to_ends_on >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', ends_on)), (CASE WHEN is_welcome5 >= 72 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN months_starts_on_to_ends_on >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END))
)
SELECT spring10_max_is_welcome5, grouping_level
FROM daily
ORDER BY grouping_level

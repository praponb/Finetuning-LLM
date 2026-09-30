WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        LAG(discount_pct, 2, 0) OVER w_grp AS prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on ASC, id ASC) AS seq_no
    FROM promotions
    WHERE promo_code IN ('VIP', 'SPRING10')
    WINDOW w_grp AS (PARTITION BY YEAR(starts_on), channel ORDER BY discount_pct ASC, id ASC)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(promo_code)) AS promo_code_title,
        CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10
    FROM scored
    WHERE ends_on > DATE '2023-10-25'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        promo_code,
        prev_discount_pct,
        is_spring10
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY prev_discount_pct ASC, id ASC) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(promo_code) AS promo_code_length
    FROM latest
    WHERE is_spring10 != 39
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(prev_discount_pct) OVER w_part AS next_prev_discount_pct,
        LAG(is_spring10) OVER w_part AS prev_is_spring10,
        LEAD(is_spring10) OVER w_part AS next_is_spring10,
        ROW_NUMBER() OVER w_part AS event_seq,
        LAST_VALUE(prev_discount_pct) IGNORE NULLS OVER (ORDER BY is_spring10 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_discount_pct
    FROM cleaned
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY prev_discount_pct ASC, id ASC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_is_spring10 >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_is_spring10_tier,
    SUM(prev_discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_total_prev_discount_pct,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY CUBE (CASE WHEN next_is_spring10 >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY spring10_total_prev_discount_pct ASC

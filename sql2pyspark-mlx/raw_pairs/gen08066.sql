WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        DATE_TRUNC('YEAR', starts_on) AS starts_on_period
    FROM promotions
    WHERE ends_on >= DATE '2023-07-27'
      AND discount_pct >= 0.18
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        starts_on,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct
    FROM base
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        starts_on,
        running_sum_discount_pct,
        running_count_discount_pct,
        running_avg_discount_pct,
        CASE WHEN running_avg_discount_pct >= 0.29 THEN 'High' WHEN running_avg_discount_pct >= 0.19 THEN 'Medium' ELSE 'Low' END AS running_avg_discount_pct_tier,
        FLOOR(running_avg_discount_pct) AS running_avg_discount_pct_whole
    FROM metrics
    WHERE starts_on >= DATE '2023-07-19'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(running_count_discount_pct, 0) AS running_count_discount_pct_rounded
    FROM base_promotions
    WHERE starts_on >= DATE '2023-08-01'
)
SELECT
    id,
    starts_on,
    running_sum_discount_pct,
    running_avg_discount_pct,
    running_avg_discount_pct_tier,
    running_avg_discount_pct_whole,
    running_count_discount_pct_rounded
FROM cleaned
ORDER BY running_count_discount_pct_rounded NULLS LAST, running_avg_discount_pct DESC;

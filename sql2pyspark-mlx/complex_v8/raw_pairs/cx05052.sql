WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE holder_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        start_date_label,
        premium_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Label and score rows
    SELECT *, ABS(premium - 2096.6) AS premium_distance
    FROM deduped
    WHERE holder_id >= 19
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        premium_distance,
        NTILE(4) OVER (PARTITION BY MONTH(start_date), product ORDER BY premium DESC, id) AS premium_quartile,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date), product ORDER BY premium DESC, id) AS premium_seq,
        MAX(premium_adj) OVER (PARTITION BY MONTH(start_date), product ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_adj_max_7,
        NTH_VALUE(premium, 2) OVER (PARTITION BY MONTH(start_date), product ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        NTILE(5) OVER w_start_date_month_product_by AS premium_quintile
    FROM labeled
    WINDOW w_start_date_month_product_by AS (PARTITION BY MONTH(start_date), product ORDER BY premium DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        premium_quartile,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        MIN(start_date) AS first_start_date,
        MIN(second_premium) AS min_second_premium,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        ROUND(SUM(premium) / NULLIF(SUM(premium_adj_max_7), 0), 3) AS premium_to_premium_adj_max_7_ratio,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        MAX(start_date) AS last_start_date
    FROM windowed
    GROUP BY premium_quartile, DATE_FORMAT(start_date, 'yyyy-MM')
    HAVING AVG(premium_quartile) > 4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        premium_quartile,
        start_date_ym,
        first_start_date,
        median_premium,
        premium_to_premium_adj_max_7_ratio,
        last_start_date,
        start_date_span_days - LAG(start_date_span_days) OVER w_premium_quartile_by_first_st AS start_date_span_days_change,
        SUM(min_second_premium) OVER (PARTITION BY premium_quartile) AS premium_quartile_sum_min_second_premium
    FROM totals
    WINDOW w_premium_quartile_by_first_st AS (PARTITION BY premium_quartile ORDER BY first_start_date DESC, premium_quartile, start_date_ym)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE median_premium > (SELECT AVG(median_premium) FROM with_windows)
)
-- Final projection
SELECT
    premium_quartile,
    median_premium,
    last_start_date,
    start_date_span_days_change,
    premium_quartile_sum_min_second_premium,
    DATE_TRUNC('YEAR', last_start_date) AS last_start_date_year
FROM filtered
WHERE start_date_span_days_change BETWEEN -280 AND 350
ORDER BY median_premium, premium_quartile

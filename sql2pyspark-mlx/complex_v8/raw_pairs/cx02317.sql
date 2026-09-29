WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        LAST_DAY(end_date) AS end_date_month_end
    FROM policies
    WHERE product = 'TRAVEL'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        start_date,
        product_slug,
        end_date_month_end,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY end_date DESC, id)) AS days_since_prev_start_date,
        RANK() OVER w_all_by_end_date_month_end_ti AS end_date_month_end_rank,
        DATEDIFF(end_date_month_end, LAG(end_date_month_end) OVER w_start_date_month_by_end_date) AS days_since_prev_end_date_month_end,
        ROUND(CUME_DIST() OVER (ORDER BY start_date), 3) AS start_date_cume_dist,
        NTH_VALUE(end_date, 2) OVER (ORDER BY end_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date, id), 3) AS start_date_percent_rank,
        LEAD(start_date) OVER (ORDER BY end_date_month_end, id) AS next_start_date,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY end_date DESC, id)) AS days_since_prev_end_date,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LEAD(start_date) OVER (ORDER BY end_date DESC, id) AS next_start_date_2,
        RANK() OVER (ORDER BY start_date) AS start_date_rank,
        NTILE(4) OVER w_start_date_month_by_end_date AS end_date_quartile
    FROM base_policys
    WINDOW w_all_by_end_date_month_end_ti AS (ORDER BY end_date_month_end), w_start_date_month_by_end_date AS (PARTITION BY MONTH(start_date) ORDER BY end_date DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', end_date_month_end) AS days_since_end_date_month_end
    FROM metrics
    WHERE end_date_month_end_rank >= 2
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        end_date_quartile - LAG(end_date_quartile) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month_end) ORDER BY next_start_date_2 DESC, id) AS end_date_quartile_change,
        MAX(end_date_quartile) OVER (PARTITION BY YEAR(second_end_date)) AS second_end_date_year_max_end_date_quartile,
        COUNT(*) OVER (PARTITION BY YEAR(second_end_date)) AS rows_in_second_end_date_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month_end) ORDER BY next_start_date_2 DESC, id), 3) AS next_start_date_2_percent_rank,
        ROUND(AVG(start_date_percent_rank) OVER w_second_end_date_year, 2) AS second_end_date_year_avg_start_date_percent_rank,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month_end) ORDER BY next_start_date_2 DESC, id) AS next_start_date_2_dense_rank,
        LEAD(start_date_percent_rank, 3) OVER (PARTITION BY MONTH(next_start_date_2), start_date_rank ORDER BY end_date_month_end_rank DESC, id) AS next3_start_date_percent_rank,
        RANK() OVER (PARTITION BY MONTH(next_start_date_2), start_date_rank ORDER BY end_date_month_end_rank DESC, id) AS end_date_month_end_rank_rank
    FROM annotated
    WINDOW w_second_end_date_year AS (PARTITION BY YEAR(second_end_date))
)
SELECT
    id,
    start_date,
    product_slug,
    end_date_month_end,
    days_since_prev_start_date,
    end_date_month_end_rank,
    days_since_prev_end_date_month_end,
    start_date_cume_dist,
    next_start_date,
    second_start_date,
    next_start_date_2,
    end_date_quartile,
    days_since_end_date_month_end,
    second_end_date_year_max_end_date_quartile,
    rows_in_second_end_date_year,
    next_start_date_2_percent_rank,
    next_start_date_2_dense_rank
FROM metrics_2
WHERE days_since_prev_end_date_month_end BETWEEN 40 AND 130
ORDER BY second_start_date, next_start_date_2 DESC NULLS FIRST

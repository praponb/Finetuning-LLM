WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATE_TRUNC('MONTH', start_date) AS start_date_month
    FROM policies
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM scoped_policys
    WHERE status_or_unknown IN ('LAPSED', 'CANCELLED', 'EXPIRED')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        end_date,
        status,
        status_or_unknown,
        start_date_month,
        FIRST_VALUE(premium) OVER (ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status_or_unknown)) AS distinct_holder_id_in_status_or_unknown,
        ROUND(CUME_DIST() OVER w_all_by_start_date_ties, 3) AS start_date_cume_dist,
        SUM(premium) OVER (PARTITION BY status_or_unknown) AS status_or_unknown_sum_premium
    FROM enriched
    WINDOW w_all_by_start_date_ties AS (ORDER BY start_date ASC NULLS LAST)
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        start_date,
        status,
        status_or_unknown,
        start_date_month,
        first_premium,
        distinct_holder_id_in_status_or_unknown,
        start_date_cume_dist,
        ROUND(first_premium - AVG(first_premium) OVER (PARTITION BY YEAR(end_date)), 2) AS first_premium_vs_end_date_year_avg,
        LAG(first_premium) OVER w_start_date_month_by_start_da AS prev_first_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date ASC), 3) AS start_date_cume_dist_2,
        COUNT(*) OVER (PARTITION BY YEAR(end_date)) AS rows_in_end_date_year,
        LEAD(start_date_month) OVER w_start_date_month_by_start_da AS next_start_date_month,
        SUM(start_date_cume_dist) OVER (PARTITION BY YEAR(end_date)) AS end_date_year_sum_start_date_cume_dist
    FROM metrics
    WINDOW w_start_date_month_by_start_da AS (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date ASC, id)
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(prev_first_premium - 2319.8) AS prev_first_premium_distance,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown_2,
        LEAST(rows_in_end_date_year, prev_first_premium) AS least_rows_in_end_date_year_prev_first_premium
    FROM metrics_2
)
-- Final projection
SELECT
    id,
    start_date,
    status_or_unknown,
    start_date_month,
    first_premium,
    distinct_holder_id_in_status_or_unknown,
    start_date_cume_dist,
    first_premium_vs_end_date_year_avg,
    prev_first_premium,
    start_date_cume_dist_2,
    rows_in_end_date_year,
    next_start_date_month,
    prev_first_premium_distance,
    status_or_unknown_2
FROM enriched_2
ORDER BY start_date_month DESC, start_date

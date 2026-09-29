WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATE_TRUNC('WEEK', end_date) AS end_date_week,
        ABS(premium - 3532.7) AS premium_distance
    FROM policies
    WHERE YEAR(end_date) = 2026
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(end_date_week) AS end_date_week_year,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        MIN(premium_distance) AS min_premium_distance,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM recent_policys
    GROUP BY 1
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE min_premium_distance > (SELECT AVG(min_premium_distance) FROM rollup_base)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(n_rows / NULLIF(distinct_holder_ids, 0), 3) AS n_rows_per_distinct_holder_ids
    FROM screened
),
derived AS (
    -- Label and score rows
    SELECT
        end_date_week_year,
        n_rows,
        distinct_holder_ids,
        min_premium_distance,
        product_list,
        n_rows_per_distinct_holder_ids,
        ROUND(min_premium_distance * 0.25, 2) AS min_premium_distance_adj
    FROM labeled
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        end_date_week_year,
        n_rows,
        distinct_holder_ids,
        min_premium_distance_adj,
        ROW_NUMBER() OVER w_end_date_week_year_by_n_rows AS seq_no,
        ROUND(PERCENT_RANK() OVER w_end_date_week_year_by_n_rows_2, 3) AS n_rows_percent_rank,
        MIN(distinct_holder_ids) OVER (PARTITION BY end_date_week_year) AS end_date_week_year_min_distinct_holder_ids,
        ROUND(CUME_DIST() OVER w_end_date_week_year_by_n_rows, 3) AS n_rows_cume_dist
    FROM derived
    WINDOW w_end_date_week_year_by_n_rows AS (PARTITION BY end_date_week_year ORDER BY n_rows DESC, end_date_week_year), w_end_date_week_year_by_n_rows_2 AS (PARTITION BY end_date_week_year ORDER BY n_rows DESC)
)
-- Final projection
SELECT
    end_date_week_year,
    n_rows,
    min_premium_distance_adj,
    seq_no,
    n_rows_percent_rank,
    end_date_week_year_min_distinct_holder_ids,
    n_rows_cume_dist
FROM metrics
ORDER BY min_premium_distance_adj DESC, n_rows_percent_rank

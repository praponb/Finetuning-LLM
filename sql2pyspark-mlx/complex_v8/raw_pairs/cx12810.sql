WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE premium > 2289.0
      AND product <> 'AUTO'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        premium,
        start_date,
        product_slug,
        days_start_date_to_end_date,
        SUBSTRING(TRIM(product_slug), 1, 3) AS product_slug_prefix
    FROM recent
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(days_start_date_to_end_date) OVER (PARTITION BY YEAR(start_date) ORDER BY start_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_start_date_to_end_date_avg_3_prior,
        ROUND(AVG(days_start_date_to_end_date) OVER w_start_date_year_by_start_dat_2, 2) AS start_date_year_avg_days_start_date_to_end_date,
        DENSE_RANK() OVER (PARTITION BY YEAR(start_date) ORDER BY start_date NULLS LAST, id) AS start_date_dense_rank,
        LEAD(start_date) OVER w_start_date_year_by_days_star AS next_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(start_date)), 0), 4) AS premium_share_of_start_date_year
    FROM tagged
    WINDOW w_start_date_year_by_start_dat_2 AS (PARTITION BY YEAR(start_date)), w_start_date_year_by_days_star AS (PARTITION BY YEAR(start_date) ORDER BY days_start_date_to_end_date DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(next_start_date) AS next_start_date_year,
        MAX(next_start_date) AS last_next_start_date,
        ROUND(STDDEV(start_date_dense_rank), 2) AS stddev_start_date_dense_rank,
        MAX(start_date) AS last_start_date
    FROM with_trends
    GROUP BY 1
    HAVING MAX(start_date_dense_rank) > 3
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(last_next_start_date, 'yyyy-MM-dd') AS last_next_start_date_label
    FROM grouped
),
labeled AS (
    -- Add calculated columns
    SELECT
        next_start_date_year,
        last_next_start_date,
        stddev_start_date_dense_rank,
        last_next_start_date_label,
        DATE_FORMAT(last_start_date, 'yyyy-MM-dd') AS last_start_date_label,
        ABS(next_start_date_year - 2025) AS next_start_date_year_distance
    FROM annotated
    WHERE next_start_date_year < 2025
)
SELECT
    next_start_date_year,
    last_next_start_date,
    stddev_start_date_dense_rank,
    last_next_start_date_label,
    next_start_date_year_distance,
    ADD_MONTHS(last_next_start_date, 6) AS last_next_start_date_plus_6m,
    DATE_TRUNC('QUARTER', last_next_start_date) AS last_next_start_date_quarter
FROM labeled

WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        YEAR(end_date) AS end_date_year
    FROM policies
    WHERE product = 'HEALTH'
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM scoped_policys
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        end_date_year,
        end_date_label,
        LEAD(end_date, 3) OVER (ORDER BY start_date ASC, id) AS next3_end_date,
        MAX(premium) OVER (PARTITION BY end_date_year, MONTH(start_date) ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY end_date_year, MONTH(start_date) ORDER BY premium DESC NULLS FIRST), 3) AS premium_percent_rank
    FROM derived
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        end_date_year,
        next3_end_date,
        premium_max_4,
        premium_percent_rank,
        CONCAT_WS('-', end_date_label, product) AS end_date_label_product_label,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM with_windows
    WHERE start_date < DATE '2025-07-07'
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', end_date_quarter) AS days_since_end_date_quarter,
        GREATEST(premium_percent_rank, end_date_year) AS greatest_premium_percent_rank_end_date_year,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM annotated
    WHERE YEAR(start_date) = 2025
),
annotated_2 AS (
    -- Label and score rows
    SELECT
        id,
        start_date,
        end_date,
        end_date_year,
        premium_max_4,
        premium_percent_rank,
        end_date_label_product_label,
        days_since_end_date_quarter,
        greatest_premium_percent_rank_end_date_year,
        DATE_TRUNC('WEEK', end_date) AS end_date_week,
        DATEDIFF(next3_end_date, start_date) AS days_start_date_to_next3_end_date,
        DATE_TRUNC('HOUR', end_date_quarter) AS end_date_quarter_hour
    FROM derived_2
)
-- Final projection
SELECT
    id,
    end_date,
    end_date_year,
    premium_max_4,
    premium_percent_rank,
    end_date_label_product_label,
    days_since_end_date_quarter,
    greatest_premium_percent_rank_end_date_year,
    end_date_week,
    days_start_date_to_next3_end_date,
    end_date_quarter_hour,
    COALESCE(end_date_year, 0) AS end_date_year_filled
FROM annotated_2
ORDER BY greatest_premium_percent_rank_end_date_year DESC

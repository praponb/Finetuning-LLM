WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status = 'ACTIVE'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        COUNT(*) FILTER (WHERE status NOT IN ('LAPSED', 'EXPIRED')) AS n_status_not_in_lapsed_expired,
        MIN(start_date) AS first_start_date,
        COUNT(*) AS n_rows,
        SUM(premium) AS total_premium,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        MAX(start_date) AS last_start_date
    FROM prepared
    GROUP BY DATE_TRUNC('MONTH', end_date)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE n_rows > (SELECT MIN(n_rows) FROM summary)
)
-- Final projection
SELECT
    end_date_month,
    n_status_not_in_lapsed_expired,
    distinct_holder_ids,
    QUARTER(last_start_date) AS last_start_date_quarter,
    ROUND(total_premium * 0.25, 2) AS total_premium_adj
FROM filtered
ORDER BY end_date_month

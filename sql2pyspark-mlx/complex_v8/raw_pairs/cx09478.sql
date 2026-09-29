WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE product = 'HOME'
      AND premium > 2111.3
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        status,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS n_status_active,
        COUNT(*) AS row_count,
        MAX(end_date) AS last_end_date
    FROM prepared_policys
    GROUP BY product, status
),
annotated AS (
    -- Add calculated columns
    SELECT
        product,
        status,
        n_status_active,
        row_count,
        last_end_date,
        DATE_TRUNC('MONTH', last_end_date) AS last_end_date_month,
        ABS(n_status_active - 5) AS n_status_active_distance
    FROM per_group
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, product, n_status_active_distance
    FROM annotated
) src
PIVOT (
    SUM(n_status_active_distance) AS amount, COUNT(n_status_active_distance) AS n
    FOR product IN ('AUTO', 'HEALTH', 'HOME', 'LIFE')
)

WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status <> 'CANCELLED'
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM scoped
    WHERE premium < (SELECT MAX(premium) FROM scoped)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, product, premium
    FROM qualified
) src
PIVOT (
    AVG(premium)
    FOR product IN ('HOME', 'TRAVEL', 'HEALTH')
)
ORDER BY end_date

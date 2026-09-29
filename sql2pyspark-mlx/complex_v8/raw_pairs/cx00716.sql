WITH valid_policys AS (
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
    WHERE (holder_id <= 9 OR end_date >= DATE '2025-11-20')
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM valid_policys
    WHERE premium < (SELECT MAX(premium) FROM valid_policys)
)
SELECT *
FROM (
    SELECT end_date, product, premium
    FROM screened
) src
PIVOT (
    MAX(premium)
    FOR product IN ('LIFE', 'AUTO', 'HOME', 'TRAVEL')
)
ORDER BY end_date

WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date IS NOT NULL
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.region AS holder_region
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        ABS(premium - 3521.8) AS premium_distance,
        DATE_ADD(end_date, 30) AS end_date_plus_30d
    FROM combined_details_holder
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE premium > (SELECT MIN(premium) FROM enriched)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT status, end_date, holder_region, premium_distance
    FROM without_matches
) src
PIVOT (
    AVG(premium_distance)
    FOR holder_region IN ('West', 'East', 'North', 'Central')
)
ORDER BY status, end_date

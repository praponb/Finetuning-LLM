WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        QUARTER(end_date) AS end_date_quarter
    FROM policies
    WHERE end_date BETWEEN DATE '2025-10-12' AND DATE '2027-01-03'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        end_date_quarter,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM cleaned_policys
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium >= 1132.1
        )
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date_quarter,
        s.days_start_date_to_end_date,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM filtered s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claim_type = 'collision'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, claim_type, end_date_quarter
    FROM with_children
) src
PIVOT (
    SUM(end_date_quarter)
    FOR claim_type IN ('collision', 'flood')
)
ORDER BY holder_id

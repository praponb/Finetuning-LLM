WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM policies
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.status_group,
        s.start_date_quarter,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM recent s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-05-01'
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM joined_holder s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
      AND s.status_group = 'group_b'
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.start_date_quarter,
        p.holder_name,
        p.region AS holder_region
    FROM screened s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date_quarter,
    holder_name,
    holder_region
FROM combined_details_holder
ORDER BY holder_region, id DESC

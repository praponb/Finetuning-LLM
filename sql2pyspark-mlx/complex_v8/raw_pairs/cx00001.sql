WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        ABS(premium - 3465.4) AS premium_distance,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE premium >= 2096.6
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_distance,
        s.status_or_unknown,
        p.segment AS holder_segment,
        p.holder_name
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_details AS (
    -- Attach holder attributes
    SELECT s.*, p.holder_name AS holder_holder_name, p.region AS holder_region
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IN ('PAID', 'APPROVED')
              AND c.claimed_amount <= 31752.9
        )
)
SELECT
    id,
    holder_id,
    product,
    premium_distance,
    status_or_unknown,
    holder_segment,
    holder_name,
    holder_holder_name,
    holder_region,
    LOWER(TRIM(holder_segment)) AS holder_segment_clean
FROM screened
WHERE premium_distance BETWEEN 1500.0 AND 4250.0
ORDER BY product, id
LIMIT 5

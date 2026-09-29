WITH cleaned AS (
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
    WHERE holder_id <= 14
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        COUNT(*) FILTER (WHERE status NOT IN ('REJECTED', 'OPEN')) AS claim_status_count,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.last_claim_date
    FROM cleaned s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, product, premium
    FROM with_stats
) src
PIVOT (
    MAX(premium)
    FOR product IN ('TRAVEL', 'LIFE')
)
ORDER BY start_date

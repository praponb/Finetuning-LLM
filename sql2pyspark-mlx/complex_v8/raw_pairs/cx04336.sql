WITH valid_policys AS (
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
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS claim_status_count,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) FILTER (WHERE status = 'APPROVED') AS claim_status_count_2
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.last_claim_date,
        COALESCE(c.claim_status_count_2, 0) AS claim_status_count_2
    FROM valid_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_claim_date, holder_id, product, premium
    FROM summarized_rows
) src
PIVOT (
    AVG(premium)
    FOR product IN ('TRAVEL', 'AUTO', 'HEALTH', 'HOME')
)
ORDER BY last_claim_date, holder_id

WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        s.approved_amount_filled,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Travel', 'Health', 'Motor')
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_claimed_amount,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS min_claim_approved_amount
    FROM combined_details_adjuster s
)
SELECT *
FROM (
    SELECT adjuster_team, claim_type, adjuster_level
    FROM profiled
) src
PIVOT (
    AVG(adjuster_level)
    FOR claim_type IN ('delay', 'flood')
)

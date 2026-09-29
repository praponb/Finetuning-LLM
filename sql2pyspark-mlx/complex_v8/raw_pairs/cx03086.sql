WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_parent_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_len,
        a.adjuster_name
    FROM scoped_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM with_parent_adjuster s
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN status IN ('IN_REVIEW', 'OPEN') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM summarized_rows
),
with_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_len,
        s.adjuster_name,
        s.claim_count,
        s.status_group,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level
    FROM annotated s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.premium >= 1042.8
),
adjuster_id_stats AS (
    -- Avg policy_premium per adjuster_id
    SELECT adjuster_id, AVG(policy_premium) AS avg_policy_premium
    FROM with_details
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_policy_premium,
        ROUND(s.policy_premium - g.avg_policy_premium, 2) AS policy_premium_minus_avg
    FROM with_details s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.policy_premium > g.avg_policy_premium
)
SELECT *
FROM (
    SELECT policy_id, status, claim_count
    FROM outliers
) src
PIVOT (
    SUM(claim_count)
    FOR status IN ('PAID', 'IN_REVIEW', 'REJECTED')
)
ORDER BY policy_id

WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        QUARTER(claim_date) AS claim_date_quarter,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE claim_type = 'theft'
      AND status = 'PAID'
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.claim_date_quarter,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.status AS policy_status,
        p.premium AS policy_premium
    FROM valid_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM combined_details_adjuster
    WHERE policy_premium < 2945.9
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.max_claim_approved_amount
    FROM scored s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT *
FROM (
    SELECT last_claim_reported_at, claim_date_quarter, policy_status, adjuster_level
    FROM with_stats
) src
PIVOT (
    SUM(adjuster_level)
    FOR policy_status IN ('ACTIVE', 'LAPSED', 'CANCELLED')
)
ORDER BY last_claim_reported_at, claim_date_quarter

WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount <= 17759.3
),
claim_type_stats AS (
    -- Avg claimed_amount per claim_type
    SELECT claim_type, AVG(claimed_amount) AS avg_claimed_amount
    FROM prepared
    GROUP BY claim_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM prepared s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claimed_amount > g.avg_claimed_amount
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.avg_claimed_amount,
        s.claimed_amount_minus_avg,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM vs_group s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_date, status, claim_type, avg_claimed_amount
    FROM with_children_claims
) src
PIVOT (
    SUM(avg_claimed_amount) AS amount, COUNT(avg_claimed_amount) AS n
    FOR claim_type IN ('flood', 'theft', 'fire')
)

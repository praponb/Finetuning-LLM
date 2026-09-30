WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 13:00:00'
),
adjuster_id_stats AS (
    -- Avg claimed_amount per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount) AS avg_claimed_amount
    FROM valid
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM valid s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount > g.avg_claimed_amount
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.status_slug,
        s.greatest_approved_amount_claimed_amount,
        s.avg_claimed_amount,
        s.claimed_amount_minus_avg,
        a.team AS adjuster_team,
        p.start_date AS policy_start_date
    FROM above_average s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date >= DATE '2024-03-24'
        )
)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount_minus_avg, approved_amount, greatest_approved_amount_claimed_amount
    FROM qualified
) src
UNPIVOT (
    value FOR field IN (claimed_amount_minus_avg, approved_amount, greatest_approved_amount_claimed_amount)
) unp

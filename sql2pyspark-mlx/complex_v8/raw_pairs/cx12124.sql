WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 12:00:00'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.greatest_claimed_amount_approved_amount,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM valid_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        claim_status,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(greatest_claimed_amount_approved_amount), 2) AS avg_greatest_claimed_amount_approved_amount,
        MAX(greatest_claimed_amount_approved_amount) AS max_greatest_claimed_amount_approved_amount
    FROM exploded_detail_claims
    WHERE greatest_claimed_amount_approved_amount < 42500.0
    GROUP BY adjuster_id, claim_status
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.adjuster_id,
        s.claim_status,
        s.row_count,
        s.distinct_ids,
        s.avg_greatest_claimed_amount_approved_amount,
        s.max_greatest_claimed_amount_approved_amount,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM grouped s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT adjuster_id, claim_status, field, value
FROM (
    SELECT adjuster_id, claim_status, avg_greatest_claimed_amount_approved_amount, max_greatest_claimed_amount_approved_amount
    FROM combined_details
) src
UNPIVOT (
    value FOR field IN (avg_greatest_claimed_amount_approved_amount, max_greatest_claimed_amount_approved_amount)
) unp

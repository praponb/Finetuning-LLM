WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount
    FROM claims
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(CASE WHEN c.status IN ('OPEN', 'PAID') THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_date
    FROM base s
),
enriched AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM summarized_rows s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', first_claim_date) AS first_claim_date_month,
        SUM(approved_amount) AS total_approved_amount,
        SUM(claim_count) AS total_claim_count
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', first_claim_date)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM per_group
    WHERE total_claim_count < (SELECT MAX(total_claim_count) FROM per_group)
)
-- Final projection
SELECT
    first_claim_date_month,
    total_approved_amount,
    total_claim_count,
    ROUND(total_approved_amount * 0.25, 2) AS total_approved_amount_adj
FROM without_matches
ORDER BY total_approved_amount_adj, total_approved_amount DESC NULLS FIRST

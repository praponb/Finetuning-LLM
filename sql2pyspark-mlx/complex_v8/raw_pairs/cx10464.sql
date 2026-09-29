WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
    WHERE status = 'APPROVED'
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date
    FROM valid s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_claimed_amount
    FROM with_children_claims s
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.team AS adjuster_team, a.level AS adjuster_level
    FROM with_counts s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        MAX(total_claim_claimed_amount) AS max_total_claim_claimed_amount,
        ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
        SUM(total_claim_claimed_amount) AS total_total_claim_claimed_amount,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
    FROM enriched_adjuster
    GROUP BY DATE_TRUNC('MONTH', claim_date)
)
-- Final projection
SELECT
    claim_date_month,
    max_total_claim_claimed_amount,
    total_total_claim_claimed_amount,
    claim_date_span_days,
    COALESCE(stddev_claimed_amount, 0.0) AS stddev_claimed_amount_filled
FROM totals
ORDER BY total_total_claim_claimed_amount, claim_date_month DESC NULLS FIRST

WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE approved_amount <= 14993.0
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_claimed_amount
    FROM cleaned_claims s
),
derived AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM with_stats
)
SELECT
    adjuster_id,
    policy_id,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    MAX(days_since_claim_date) AS max_days_since_claim_date
FROM derived
GROUP BY ROLLUP(adjuster_id, policy_id)
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST

WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        MONTH(reported_at) AS reported_at_month,
        ABS(claimed_amount - 29336.0) AS claimed_amount_distance
    FROM claims
    WHERE status <> 'REJECTED'
      AND claim_date IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        least_claimed_amount_approved_amount,
        reported_at_month,
        claimed_amount_distance,
        TO_DATE(reported_at) AS reported_at_date,
        ABS(reported_at_month - 6) AS reported_at_month_distance,
        DATE_TRUNC('WEEK', reported_at) AS reported_at_week
    FROM cleaned
    WHERE claim_type IN ('medical', 'theft', 'delay')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM enriched s
)
SELECT *
FROM (
    SELECT reported_at_month_distance, claim_type, claimed_amount
    FROM with_counts
) src
PIVOT (
    COUNT(claimed_amount)
    FOR claim_type IN ('delay', 'medical', 'fire')
)

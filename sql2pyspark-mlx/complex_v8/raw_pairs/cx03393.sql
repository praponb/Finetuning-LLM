WITH cleaned AS (
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
        reported_at
    FROM claims
    WHERE claimed_amount >= 5774.6
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        TO_DATE(reported_at) AS reported_at_date
    FROM cleaned
    WHERE claimed_amount > 3703.0
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status IN ('IN_REVIEW', 'REJECTED') THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_status_count
    FROM scored s
),
enriched AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        p.start_date AS policy_start_date,
        p.product AS policy_product
    FROM summarized_rows s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team <> 'Property'
    JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    least_claimed_amount_approved_amount,
    reported_at_date,
    claim_status_count,
    policy_product,
    LENGTH(TRIM(status)) AS status_len
FROM enriched
ORDER BY status DESC

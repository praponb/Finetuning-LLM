WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        adjuster_id,
        claim_type,
        product,
        QUARTER(claim_date) AS claim_date_quarter,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter_2
    FROM in_window
    WHERE policy_id >= 24
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.level AS adjuster_level, p.end_date AS policy_end_date
    FROM derived s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT *
FROM (
    SELECT policy_id, claim_type, adjuster_level
    FROM combined_details_adjuster
) src
PIVOT (
    MIN(adjuster_level)
    FOR claim_type IN ('theft', 'medical')
)
ORDER BY policy_id

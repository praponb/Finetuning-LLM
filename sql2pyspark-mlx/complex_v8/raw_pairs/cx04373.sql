WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.status = 'OPEN') FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM joined_ranges s
)
SELECT
    id,
    claim_date,
    approved_amount,
    status,
    premium,
    claim_status_count,
    last_claim_date,
    ABS(claim_status_count - 3) AS claim_status_count_distance
FROM summarized_rows
ORDER BY premium DESC

WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.premium,
        a.adjuster_name
    FROM in_window s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Health', 'Property', 'Travel')
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 5
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.premium,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM joined s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
SELECT
    status,
    claim_id,
    adjuster_id,
    GROUPING(status) AS is_total_status,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM exploded_detail
GROUP BY CUBE(status, claim_id, adjuster_id)
ORDER BY status NULLS LAST, claim_id NULLS LAST, adjuster_id NULLS LAST

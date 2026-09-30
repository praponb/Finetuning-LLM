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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM in_window s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.approved_amount >= 2349.3
        )
      AND s.claimed_amount > 14019.8
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.status,
        s.product,
        s.premium,
        a.team AS adjuster_team,
        a.adjuster_name,
        p.product AS policy_product
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team NOT IN ('Travel', 'Property')
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.team IS NOT NULL
)
SELECT
    id,
    policy_id,
    claim_date,
    adjuster_id,
    status,
    product,
    premium,
    adjuster_team,
    adjuster_name,
    CONCAT_WS('|', adjuster_name, product) AS adjuster_name_product_label
FROM combined_details_adjuster
WHERE premium < 3521.8
ORDER BY adjuster_team, status, id
LIMIT 5

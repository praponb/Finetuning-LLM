WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_parent AS (
    -- Attach policy attributes
    SELECT s.*, p.start_date AS policy_start_date
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(policy_start_date) AS policy_start_date_year,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM with_parent
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    adjuster_id,
    product,
    policy_start_date,
    product_status_label,
    LENGTH(TRIM(product_status_label)) AS product_status_label_len
FROM labeled
WHERE claimed_amount BETWEEN 10518.4 AND 32441.2
ORDER BY policy_id DESC, policy_start_date

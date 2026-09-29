WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(claim_date) AS claim_date_month,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        YEAR(claim_date) AS claim_date_year
    FROM covered
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at
    FROM enriched s
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.approved_amount,
        s.adjuster_id,
        s.premium,
        s.product_slug,
        s.first_claim_reported_at,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM profiled s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claimed_amount,
    approved_amount,
    adjuster_id,
    premium,
    product_slug,
    first_claim_reported_at,
    claim_policy_id,
    claim_status,
    claim_reported_at,
    CONCAT(product_slug, '#', CAST(claim_policy_id AS STRING)) AS product_slug_ref
FROM line_level
ORDER BY policy_id DESC

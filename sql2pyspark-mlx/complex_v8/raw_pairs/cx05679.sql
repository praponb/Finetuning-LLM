WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE status = 'REJECTED'
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.product AS policy_product,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        po.holder_name,
        po.region AS holder_region
    FROM valid_claims s
    JOIN policies p ON p.id = s.policy_id AND YEAR(p.start_date) = 2025
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.status IS NOT NULL
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.status_ref,
        s.approved_amount_bucket,
        s.policy_product,
        s.policy_status,
        s.holder_name,
        s.holder_region,
        a.team AS adjuster_team
    FROM enriched_policy s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team = 'Property'
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.status_ref,
        s.policy_product,
        s.holder_name,
        s.adjuster_team,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date
    FROM with_details s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
policy_product_stats AS (
    -- Avg claim_claimed_amount per policy_product
    SELECT policy_product, AVG(claim_claimed_amount) AS avg_claim_claimed_amount
    FROM exploded_detail
    GROUP BY policy_product
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_claimed_amount,
        ROUND(s.claim_claimed_amount - g.avg_claim_claimed_amount, 2) AS claim_claimed_amount_minus_avg
    FROM exploded_detail s
    JOIN policy_product_stats g ON g.policy_product = s.policy_product
    WHERE s.claim_claimed_amount > g.avg_claim_claimed_amount
),
derived AS (
    -- Derive helper columns
    SELECT *, DATE_ADD(claim_date, 30) AS claim_date_plus_30d
    FROM above_average
),
scored AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(policy_product), 1, 2) AS policy_product_prefix
    FROM derived
    WHERE claim_claimed_amount_minus_avg > -40000.0
)
-- Subtotals with CUBE
SELECT
    policy_id,
    policy_product,
    MAX(avg_claim_claimed_amount) AS max_avg_claim_claimed_amount
FROM scored
GROUP BY CUBE(policy_id, policy_product)

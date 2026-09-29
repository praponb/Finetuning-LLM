WITH prepared_claims AS (
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
        reported_at,
        MONTH(claim_date) AS claim_date_month,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d
    FROM claims
    WHERE status IN ('OPEN', 'PAID', 'IN_REVIEW')
      AND adjuster_id >= 4
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_date_plus_90d,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM prepared_claims s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        adjuster_level - LAG(adjuster_level) OVER w_all_by_adjuster_level AS adjuster_level_change,
        NTILE(5) OVER w_all_by_adjuster_level AS adjuster_level_quintile,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY policy_product, policy_id), 2) AS adjuster_level_vs_policy_product_policy_id_avg
    FROM combined_details
    WINDOW w_all_by_adjuster_level AS (ORDER BY adjuster_level DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LAST_DAY(policy_end_date) AS policy_end_date_month_end,
        GREATEST(approved_amount, adjuster_level) AS greatest_approved_amount_adjuster_level,
        DATE_TRUNC('QUARTER', claim_date_plus_90d) AS claim_date_plus_90d_quarter
    FROM ranked
    WHERE policy_start_date IS NOT NULL
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        *,
        adjuster_level_change - approved_amount AS adjuster_level_change_minus_approved_amount,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug
    FROM derived
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    claim_date_plus_90d,
    policy_start_date,
    policy_end_date,
    policy_product,
    adjuster_level,
    adjuster_level_change,
    adjuster_level_vs_policy_product_policy_id_avg,
    greatest_approved_amount_adjuster_level,
    claim_date_plus_90d_quarter,
    adjuster_level_change_minus_approved_amount,
    QUARTER(claim_date) AS claim_date_quarter
FROM derived_2
WHERE adjuster_level < 3
ORDER BY claim_date_quarter DESC NULLS FIRST

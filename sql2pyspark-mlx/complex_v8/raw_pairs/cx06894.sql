WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix,
        CASE WHEN claim_type IN ('fire', 'delay') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-06-26' AND DATE '2025-08-25'
      AND TO_DATE(reported_at) < DATE '2025-03-08'
),
annotated AS (
    -- Derive helper columns
    SELECT *, FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM filtered
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM annotated s
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claim_type_prefix,
        s.claim_type_group,
        s.approved_amount_bucket,
        s.claim_count,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM profiled s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claimed_amount <= 42838.9
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_type_prefix,
        claim_type_group,
        approved_amount_bucket,
        claim_count,
        claim_id,
        claim_date,
        claim_claimed_amount,
        claim_reported_at,
        ROUND(AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_reported_at) ORDER BY claim_claimed_amount ASC, id, claim_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        MAX(approved_amount_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_reported_at) ORDER BY claim_claimed_amount ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_bucket_so_far,
        ROUND(AVG(claim_claimed_amount) OVER (PARTITION BY claim_count, adjuster_id ORDER BY claim_reported_at NULLS LAST, id, claim_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS claim_claimed_amount_avg_2,
        MAX(approved_amount_bucket) OVER (PARTITION BY policy_id, claim_id ORDER BY approved_amount ASC, id, claim_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_bucket_max_1_prior
    FROM exploded_detail
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_type_prefix,
        s.claim_type_group,
        s.approved_amount_bucket,
        s.claim_count,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_reported_at,
        s.max_approved_amount_bucket_so_far,
        s.claim_claimed_amount_avg_2,
        s.approved_amount_bucket_max_1_prior,
        c.id AS claim_id_2,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM sequenced s
    JOIN claims c ON c.policy_id = s.policy_id
),
filtered_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM exploded_detail_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type NOT IN ('theft', 'collision')
        )
)
SELECT
    id,
    policy_id,
    claim_type_prefix,
    claim_type_group,
    approved_amount_bucket,
    claim_id,
    claim_claimed_amount,
    claim_reported_at,
    max_approved_amount_bucket_so_far,
    claim_claimed_amount_avg_2,
    approved_amount_bucket_max_1_prior,
    claim_id_2,
    claim_type,
    claim_approved_amount,
    claim_approved_amount + claim_count AS claim_approved_amount_plus_claim_count
FROM filtered_2
ORDER BY claim_approved_amount_plus_claim_count DESC

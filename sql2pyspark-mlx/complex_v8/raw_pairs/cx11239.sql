WITH base AS (
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
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at
    FROM claims
    WHERE claim_type = 'flood'
      AND claimed_amount BETWEEN 4698.6 AND 39666.5
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IS NOT NULL) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count
    FROM base s
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM summarized_rows
),
joined_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.claim_type_len,
        s.approved_amount_bucket,
        s.days_since_reported_at,
        s.claim_claim_type_count,
        s.claim_type_prefix,
        s.status_prefix,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.premium AS policy_premium
    FROM derived s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY claim_claim_type_count DESC NULLS FIRST) AS claim_claim_type_count_rank,
        NTILE(10) OVER (PARTITION BY adjuster_team ORDER BY approved_amount_bucket DESC NULLS FIRST, id) AS approved_amount_bucket_decile,
        DENSE_RANK() OVER w_all_by_claim_claim_type_coun_2 AS claim_claim_type_count_dense_rank
    FROM joined_adjuster
    WINDOW w_all_by_claim_claim_type_coun_2 AS (ORDER BY claim_claim_type_count DESC NULLS FIRST, id)
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.claim_claim_type_count,
        s.status_prefix,
        s.adjuster_level,
        s.adjuster_name,
        s.policy_premium,
        s.approved_amount_bucket_decile,
        s.claim_claim_type_count_dense_rank,
        p.product AS policy_product
    FROM sequenced s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product = 'HEALTH'
),
summary AS (
    -- Group-level totals
    SELECT
        approved_amount_bucket_decile,
        claim_claim_type_count,
        policy_product,
        PERCENTILE_APPROX(claim_claim_type_count, 0.5) AS median_claim_claim_type_count,
        ROUND(AVG(claim_claim_type_count_dense_rank), 2) AS avg_claim_claim_type_count_dense_rank,
        COUNT(*) AS cnt,
        MIN(adjuster_level) AS min_adjuster_level,
        ROUND(SUM(claim_claim_type_count_dense_rank) / NULLIF(SUM(claim_claim_type_count), 0), 3) AS claim_claim_type_count_dense_rank_to_claim_claim
    FROM with_details_policy
    GROUP BY approved_amount_bucket_decile, claim_claim_type_count, policy_product
    HAVING AVG(claim_claim_type_count) > 2
)
-- Final projection
SELECT
    approved_amount_bucket_decile,
    claim_claim_type_count,
    policy_product,
    median_claim_claim_type_count,
    avg_claim_claim_type_count_dense_rank,
    cnt,
    min_adjuster_level,
    claim_claim_type_count_dense_rank_to_claim_claim
FROM summary
ORDER BY approved_amount_bucket_decile DESC, avg_claim_claim_type_count_dense_rank DESC NULLS FIRST

WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 24718.5
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.reported_at,
        p.start_date AS policy_start_date,
        po.region AS holder_region
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_id, holder_region ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROW_NUMBER() OVER (PARTITION BY policy_id, holder_region ORDER BY reported_at DESC, id) AS reported_at_seq,
        FIRST_VALUE(policy_start_date) OVER (PARTITION BY policy_id, holder_region ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_start_date,
        LAG(policy_start_date) OVER (ORDER BY policy_start_date DESC, id) AS prev_policy_start_date,
        ROW_NUMBER() OVER (PARTITION BY holder_region ORDER BY reported_at DESC, id) AS reported_at_seq_2,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC), 3) AS reported_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY policy_id, status ORDER BY policy_start_date, id) AS policy_start_date_dense_rank,
        LEAD(reported_at) OVER (PARTITION BY policy_id, status ORDER BY policy_start_date, id) AS next_reported_at,
        ROW_NUMBER() OVER (ORDER BY policy_start_date DESC, id) AS policy_start_date_seq
    FROM joined
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(reported_at_seq_2, reported_at_seq) AS least_reported_at_seq_2_reported_at_seq,
        reported_at_seq_2 + policy_start_date_seq AS reported_at_seq_2_plus_policy_start_date_seq,
        LOWER(REGEXP_REPLACE(TRIM(holder_region), ' ', '_')) AS holder_region_slug
    FROM with_trends
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    reported_at,
    policy_start_date,
    holder_region,
    second_reported_at,
    reported_at_seq,
    first_policy_start_date,
    reported_at_seq_2,
    policy_start_date_dense_rank,
    next_reported_at,
    policy_start_date_seq,
    least_reported_at_seq_2_reported_at_seq,
    holder_region_slug
FROM derived
WHERE TO_DATE(next_reported_at) < DATE '2025-03-02'
ORDER BY policy_start_date_dense_rank DESC

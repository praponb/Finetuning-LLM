WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        ABS(approved_amount - 9608.4) AS approved_amount_distance,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE YEAR(claim_date) = 2025
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium
    FROM recent s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product IN ('HEALTH', 'AUTO', 'HOME')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_status AS rows_in_status,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY claim_type ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        COUNT(*) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(approved_amount_distance - AVG(approved_amount_distance) OVER w_status, 2) AS approved_amount_distance_vs_status_avg,
        ROUND(AVG(policy_premium) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS policy_premium_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY policy_start_date DESC, id) AS seq_no,
        LAST_VALUE(approved_amount_distance) IGNORE NULLS OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount_distance,
        LAG(policy_start_date, 3) OVER (ORDER BY claimed_amount, id) AS prev3_policy_start_date,
        ROW_NUMBER() OVER (ORDER BY claimed_amount, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount, id) AS rn_2,
        RANK() OVER w_policy_id_by_reported_at AS reported_at_rank,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        SUM(approved_amount_distance) OVER (ORDER BY claimed_amount, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS approved_amount_distance_sum_2,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status), 0), 4) AS claimed_amount_share_of_status
    FROM enriched_policy
    WINDOW w_status AS (PARTITION BY status), w_policy_id_by_reported_at AS (PARTITION BY policy_id ORDER BY reported_at DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE rn_2 <= 5
      AND policy_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    approved_amount_distance,
    status_ref,
    policy_product,
    policy_start_date,
    policy_premium,
    rows_in_status,
    first_claimed_amount,
    running_count,
    approved_amount_distance_vs_status_avg,
    policy_premium_avg_6_prior,
    last_known_approved_amount_distance,
    prev3_policy_start_date,
    rn,
    rn_2,
    running_count_2,
    approved_amount_distance_sum_2,
    claimed_amount_share_of_status,
    CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group,
    GREATEST(running_count_2, rn) AS greatest_running_count_2_rn
FROM top_per_group
ORDER BY greatest_running_count_2_rn

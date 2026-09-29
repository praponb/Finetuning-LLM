WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        HOUR(reported_at) AS reported_at_hour,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-02-06 15:00:00'
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM valid)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status = 'OPEN' THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(CASE WHEN c.status = 'PAID' THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count_2
    FROM filtered s
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled_2,
        CASE WHEN status IN ('IN_REVIEW', 'APPROVED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM with_stats
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claim_status_count - AVG(claim_status_count) OVER (PARTITION BY policy_id, status_group), 2) AS claim_status_count_vs_policy_id_status_group_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount ASC, id), 3) AS approved_amount_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, status_group ORDER BY claim_count DESC), 3) AS claim_count_cume_dist,
        RANK() OVER (PARTITION BY policy_id, status_group ORDER BY claim_count DESC, id) AS claim_count_rank,
        LAG(approved_amount, 2) OVER (ORDER BY approved_amount ASC, id) AS prev2_approved_amount,
        MIN(claimed_amount_filled) OVER (PARTITION BY policy_id, status_group) AS policy_id_status_group_min_claimed_amount_filled
    FROM scored
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE claim_count_rank = 1
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.premium AS policy_premium
    FROM first_rows s
    JOIN policies p ON p.id = s.policy_id AND YEAR(p.start_date) = 2024
    WHERE p.start_date >= DATE '2024-08-27'
)
SELECT
    id,
    adjuster_id,
    claim_type,
    approved_amount,
    status,
    reported_at,
    reported_at_hour,
    claimed_amount_per_approved_amount,
    claimed_amount_filled,
    claim_status_count,
    claim_count,
    claim_status_count_2,
    status_group,
    claim_status_count_vs_policy_id_status_group_avg,
    approved_amount_percent_rank,
    claim_count_cume_dist,
    claim_count_rank,
    prev2_approved_amount,
    policy_id_status_group_min_claimed_amount_filled,
    policy_status,
    policy_premium,
    DATE_ADD(policy_start_date, 30) AS policy_start_date_plus_30d,
    CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
FROM with_parent
WHERE claim_status_count_2 > 3
ORDER BY claim_status_count NULLS LAST, policy_premium

WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        CASE WHEN claim_type IN ('collision', 'flood') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE MONTH(reported_at) IN (1, 3)
      AND adjuster_id <= 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(approved_amount, 1, 0) OVER w_all_by_approved_amount AS prev_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type_group, 0), 4) AS approved_amount_share_of_claim_type_group,
        ROUND(approved_amount - AVG(approved_amount) OVER w_claim_type_group_policy_id, 2) AS approved_amount_vs_claim_type_group_policy_id_av,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        MAX(approved_amount) OVER (PARTITION BY claim_type_group, policy_id) AS claim_type_group_policy_id_max_approved_amount,
        COUNT(*) OVER (PARTITION BY claim_type_group) AS rows_in_claim_type_group,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount), 3) AS approved_amount_percent_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type_group)) AS distinct_adjuster_id_in_claim_type_group,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount, id), 3) AS approved_amount_cume_dist,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id
    FROM valid_claims
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_claim_type_group AS (PARTITION BY claim_type_group), w_claim_type_group_policy_id AS (PARTITION BY claim_type_group, policy_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claim_type_group,
    claimed_amount_per_approved_amount,
    prev_approved_amount,
    approved_amount_vs_claim_type_group_policy_id_av,
    last_known_approved_amount,
    rows_in_claim_type_group,
    approved_amount_percent_rank,
    distinct_policy_id_in_adjuster_id,
    distinct_adjuster_id_in_claim_type_group,
    approved_amount_cume_dist,
    rows_in_adjuster_id
FROM with_windows
ORDER BY last_known_approved_amount DESC, rows_in_adjuster_id

WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN approved_amount >= 20880.4 THEN 'large' WHEN approved_amount >= 0.0 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_date IS NOT NULL
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.status_group,
        s.approved_amount_tier,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM filtered_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_reported_at AS seq_no,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER w_approved_amount_tier_claim_t, 0), 4) AS policy_premium_share_of_approved_amount_tier_cla,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY approved_amount_tier, claim_type)) AS distinct_policy_id_in_approved_amount_tier_claim,
        RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS claimed_amount_rank,
        ROUND(AVG(claimed_amount) OVER w_approved_amount_tier_claim_t, 2) AS approved_amount_tier_claim_type_avg_claimed_amou,
        DENSE_RANK() OVER w_approved_amount_tier_by_poli AS policy_premium_dense_rank,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY status ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        SIZE(COLLECT_SET(status) OVER w_approved_amount_tier_claim_t) AS distinct_status_in_approved_amount_tier_claim_ty,
        DENSE_RANK() OVER (PARTITION BY approved_amount_tier ORDER BY policy_premium ASC) AS policy_premium_dense_rank_2,
        MAX(policy_premium) OVER (PARTITION BY approved_amount_tier, claim_type) AS approved_amount_tier_claim_type_max_policy_premi,
        MIN(claimed_amount) OVER w_approved_amount_tier_claim_t AS approved_amount_tier_claim_type_min_claimed_amou,
        MAX(claimed_amount) OVER (PARTITION BY approved_amount_tier, claim_type) AS approved_amount_tier_claim_type_max_claimed_amou,
        ROUND(policy_premium - AVG(policy_premium) OVER w_approved_amount_tier_policy, 2) AS policy_premium_vs_approved_amount_tier_policy_id,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS claimed_amount_dense_rank
    FROM joined_policy
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at, id), w_approved_amount_tier_claim_t AS (PARTITION BY approved_amount_tier, claim_type), w_approved_amount_tier_by_poli AS (PARTITION BY approved_amount_tier ORDER BY policy_premium ASC, id), w_approved_amount_tier_policy AS (PARTITION BY approved_amount_tier, policy_id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        status_group,
        policy_end_date,
        policy_premium_share_of_approved_amount_tier_cla,
        claimed_amount_rank,
        policy_premium_dense_rank,
        second_claim_date,
        distinct_status_in_approved_amount_tier_claim_ty,
        approved_amount_tier_claim_type_max_policy_premi,
        approved_amount_tier_claim_type_min_claimed_amou,
        policy_premium_vs_approved_amount_tier_policy_id,
        claimed_amount_dense_rank,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d,
        CONCAT_WS('|', claim_type, approved_amount_tier) AS claim_type_approved_amount_tier_label
    FROM with_trends
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    policy_end_date,
    policy_premium_share_of_approved_amount_tier_cla,
    claimed_amount_rank,
    second_claim_date,
    distinct_status_in_approved_amount_tier_claim_ty,
    approved_amount_tier_claim_type_min_claimed_amou,
    policy_premium_vs_approved_amount_tier_policy_id,
    claimed_amount_dense_rank,
    reported_at_plus_7d,
    CASE WHEN claimed_amount_rank >= 8 THEN 'A' WHEN claimed_amount_rank >= 3 THEN 'B' ELSE 'C' END AS claimed_amount_rank_tier,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM derived
WHERE claim_type_approved_amount_tier_label IS NOT NULL
ORDER BY approved_amount_tier_claim_type_min_claimed_amou

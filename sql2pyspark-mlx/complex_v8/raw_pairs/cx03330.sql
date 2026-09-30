WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        CASE WHEN claimed_amount >= 28900.9 THEN 'A' WHEN claimed_amount >= 18364.1 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE YEAR(claim_date) = 2024
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(approved_amount) OVER (ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior,
        MAX(claimed_amount) OVER (ORDER BY claimed_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_max_4,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type, 0), 4) AS approved_amount_share_of_claim_type,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount DESC NULLS FIRST, id), 3) AS approved_amount_percent_rank,
        COUNT(*) OVER w_claim_type AS rows_in_claim_type
    FROM prepared
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        claimed_amount_tier,
        claimed_amount_minus_approved_amount,
        approved_amount_sum_6_prior,
        claimed_amount_max_4,
        approved_amount_share_of_claim_type,
        last_known_claimed_amount,
        approved_amount_percent_rank,
        rows_in_claim_type,
        NTH_VALUE(last_known_claimed_amount, 2) OVER (ORDER BY rows_in_claim_type ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_claimed_amount,
        MAX(approved_amount_percent_rank) OVER (PARTITION BY adjuster_id, claimed_amount_tier) AS adjuster_id_claimed_amount_tier_max_approved_amo,
        ROUND(PERCENT_RANK() OVER w_all_by_rows_in_claim_type, 3) AS rows_in_claim_type_percent_rank,
        LAG(last_known_claimed_amount) OVER (ORDER BY rows_in_claim_type ASC, id) AS prev_last_known_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id, claimed_amount_tier)) AS distinct_claim_type_in_adjuster_id_claimed_amoun,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, claimed_amount_tier)) AS distinct_policy_id_in_adjuster_id_claimed_amount
    FROM windowed
    WINDOW w_all_by_rows_in_claim_type AS (ORDER BY rows_in_claim_type ASC, id)
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 32796.4
        )
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount_tier,
        s.claimed_amount_minus_approved_amount,
        s.claimed_amount_max_4,
        s.approved_amount_share_of_claim_type,
        s.last_known_claimed_amount,
        s.approved_amount_percent_rank,
        s.rows_in_claim_type_percent_rank,
        s.distinct_claim_type_in_adjuster_id_claimed_amoun,
        s.distinct_policy_id_in_adjuster_id_claimed_amount,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM qualified s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IN ('OPEN', 'APPROVED', 'REJECTED')
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, metric, reading
FROM (
    SELECT id, claim_id, distinct_policy_id_in_adjuster_id_claimed_amount, distinct_claim_type_in_adjuster_id_claimed_amoun
    FROM with_activity
) src
UNPIVOT (
    reading FOR metric IN (distinct_policy_id_in_adjuster_id_claimed_amount, distinct_claim_type_in_adjuster_id_claimed_amoun)
) unp
ORDER BY id, claim_id, metric

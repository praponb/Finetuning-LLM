WITH valid AS (
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
        CASE WHEN claim_type IN ('flood', 'collision') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
    WHERE MONTH(reported_at) IN (1, 3)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        SIZE(COLLECT_SET(claim_type_group) OVER (PARTITION BY YEAR(claim_date))) AS distinct_claim_type_group_in_claim_date_year,
        ROW_NUMBER() OVER (ORDER BY approved_amount, id) AS approved_amount_seq,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount), 3) AS approved_amount_cume_dist
    FROM valid
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_claim_type_group_by_first_kn, 3) AS first_known_claimed_amount_percent_rank,
        LAG(approved_amount_cume_dist, 1, 0) OVER (PARTITION BY claim_type_group ORDER BY first_known_claimed_amount, id) AS prev_approved_amount_cume_dist,
        FIRST_VALUE(approved_amount_cume_dist) IGNORE NULLS OVER (PARTITION BY approved_amount_seq, MONTH(reported_at) ORDER BY approved_amount_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount_cume_dist
    FROM with_trends
    WINDOW w_claim_type_group_by_first_kn AS (PARTITION BY claim_type_group ORDER BY first_known_claimed_amount, id)
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_type_group,
        s.first_known_claimed_amount,
        s.distinct_claim_type_group_in_claim_date_year,
        s.approved_amount_seq,
        s.approved_amount_cume_dist,
        s.first_known_claimed_amount_percent_rank,
        s.first_known_approved_amount_cume_dist,
        p.product AS policy_product
    FROM metrics s
    LEFT JOIN policies p ON p.id = s.policy_id
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_policy s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
        )
      AND s.adjuster_id IS NOT NULL
)
SELECT
    id,
    adjuster_id,
    claim_date,
    approved_amount,
    distinct_claim_type_group_in_claim_date_year,
    approved_amount_seq,
    approved_amount_cume_dist,
    first_known_claimed_amount_percent_rank,
    first_known_approved_amount_cume_dist,
    policy_product,
    CONCAT(policy_product, '#', CAST(id AS STRING)) AS policy_product_ref,
    claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
FROM without_matches
ORDER BY policy_product, approved_amount_cume_dist DESC

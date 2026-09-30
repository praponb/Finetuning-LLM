WITH recent_claims AS (
    -- Scope the claims rows
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
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE adjuster_id <= 6
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        ROW_NUMBER() OVER w_policy_id_status_by_approved AS approved_amount_seq,
        COUNT(*) OVER w_claim_type AS rows_in_claim_type,
        ROUND(claimed_amount_filled - AVG(claimed_amount_filled) OVER (PARTITION BY claim_type), 2) AS claimed_amount_filled_vs_claim_type_avg,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type), 0), 4) AS approved_amount_share_of_claim_type,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type)) AS distinct_policy_id_in_claim_type,
        MAX(claimed_amount) OVER w_claim_type AS claim_type_max_claimed_amount,
        ROUND(claimed_amount_filled / NULLIF(SUM(claimed_amount_filled) OVER (PARTITION BY claim_type), 0), 4) AS claimed_amount_filled_share_of_claim_type,
        SUM(claimed_amount_filled) OVER w_claim_type AS claim_type_sum_claimed_amount_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, status ORDER BY claimed_amount, id), 3) AS claimed_amount_cume_dist,
        DENSE_RANK() OVER (PARTITION BY policy_id, status ORDER BY claimed_amount, id) AS claimed_amount_dense_rank,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type, 2) AS claimed_amount_vs_claim_type_avg,
        claimed_amount - LAG(claimed_amount) OVER (ORDER BY claim_date DESC, id) AS claimed_amount_change
    FROM recent_claims
    WINDOW w_policy_id_status_by_approved AS (PARTITION BY policy_id, status ORDER BY approved_amount, id), w_claim_type AS (PARTITION BY claim_type)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        claim_type,
        approved_amount,
        approved_amount_seq,
        rows_in_claim_type,
        claimed_amount_filled_vs_claim_type_avg,
        distinct_policy_id_in_claim_type,
        claim_type_max_claimed_amount,
        claimed_amount_filled_share_of_claim_type,
        claimed_amount_cume_dist,
        claimed_amount_vs_claim_type_avg,
        claimed_amount_change,
        ROUND(claimed_amount_change * 0.9, 2) AS claimed_amount_change_adj,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        ABS(rows_in_claim_type - 10) AS rows_in_claim_type_distance
    FROM metrics
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_type,
        rows_in_claim_type,
        claimed_amount_filled_vs_claim_type_avg,
        distinct_policy_id_in_claim_type,
        claimed_amount_filled_share_of_claim_type,
        claimed_amount_vs_claim_type_avg,
        claimed_amount_change,
        claimed_amount_change_adj,
        claim_type_clean,
        rows_in_claim_type_distance,
        FIRST_VALUE(rows_in_claim_type_distance) IGNORE NULLS OVER (PARTITION BY claim_type, approved_amount_seq ORDER BY claimed_amount_filled_share_of_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_claim_type_distance,
        SUM(claimed_amount_cume_dist) OVER (PARTITION BY approved_amount_seq, distinct_policy_id_in_claim_type) AS approved_amount_seq_distinct_policy_id_in_claim_,
        MAX(approved_amount) OVER (PARTITION BY approved_amount_seq, distinct_policy_id_in_claim_type) AS approved_amount_seq_distinct_policy_id_in_claim__2
    FROM annotated
)
-- Final projection
SELECT
    id,
    claim_type,
    rows_in_claim_type,
    claimed_amount_filled_vs_claim_type_avg,
    claimed_amount_filled_share_of_claim_type,
    claimed_amount_vs_claim_type_avg,
    claimed_amount_change,
    claim_type_clean,
    rows_in_claim_type_distance,
    first_known_rows_in_claim_type_distance,
    approved_amount_seq_distinct_policy_id_in_claim_,
    approved_amount_seq_distinct_policy_id_in_claim__2,
    LEAST(rows_in_claim_type, claimed_amount_change) AS least_rows_in_claim_type_claimed_amount_change
FROM with_trends
WHERE claimed_amount_filled_vs_claim_type_avg <= 10000.0
ORDER BY claim_type DESC

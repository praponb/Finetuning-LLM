WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, claim_type)) AS distinct_policy_id_in_adjuster_id_claim_type,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY approved_amount DESC NULLS FIRST, id), 3) AS approved_amount_cume_dist,
        ROUND(CUME_DIST() OVER w_status_claim_type_by_claim_d, 3) AS claim_date_cume_dist,
        SUM(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_approved_amount,
        NTILE(10) OVER (PARTITION BY status, claim_type ORDER BY claim_date DESC, id) AS claim_date_decile,
        LEAD(approved_amount, 3) OVER (PARTITION BY status, claim_type ORDER BY claim_date DESC, id) AS next3_approved_amount,
        approved_amount - LAG(approved_amount) OVER w_all_by_claimed_amount AS approved_amount_change,
        SIZE(COLLECT_SET(status) OVER w_claim_type) AS distinct_status_in_claim_type,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, claim_type), 2) AS approved_amount_vs_adjuster_id_claim_type_avg,
        MAX(approved_amount) OVER (ORDER BY claimed_amount ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        MAX(claimed_amount) OVER (PARTITION BY status ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_3_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type), 2) AS claimed_amount_vs_adjuster_id_claim_type_avg
    FROM filtered
    WINDOW w_status_claim_type_by_claim_d AS (PARTITION BY status, claim_type ORDER BY claim_date DESC, id), w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC NULLS LAST, id), w_claim_type AS (PARTITION BY claim_type)
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_claim_type AS rows_in_claim_type,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY distinct_status_in_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        MIN(next3_approved_amount) OVER (PARTITION BY claim_type) AS claim_type_min_next3_approved_amount,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY policy_id ORDER BY distinct_policy_id_in_adjuster_id_claim_type, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        ROUND(AVG(claimed_amount_max_3_prior) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_claimed_amount_max_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_date_decile ORDER BY distinct_status_in_claim_type, id), 3) AS distinct_status_in_claim_type_cume_dist
    FROM with_windows
    WINDOW w_claim_type AS (PARTITION BY claim_type)
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    distinct_policy_id_in_adjuster_id_claim_type,
    claim_date_cume_dist,
    claim_type_sum_approved_amount,
    claim_date_decile,
    next3_approved_amount,
    approved_amount_change,
    distinct_status_in_claim_type,
    approved_amount_vs_adjuster_id_claim_type_avg,
    max_approved_amount_so_far,
    claimed_amount_max_3_prior,
    claimed_amount_vs_adjuster_id_claim_type_avg,
    rows_in_claim_type,
    first_known_claimed_amount,
    claim_type_min_next3_approved_amount,
    claim_type_avg_claimed_amount_max_3_prior,
    distinct_status_in_claim_type_cume_dist,
    CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
FROM with_windows_2
ORDER BY status_ref, claim_date

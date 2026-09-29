WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE policy_id <= 23
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY claim_type), 2) AS claimed_amount_vs_claim_type_avg,
        DENSE_RANK() OVER (ORDER BY claimed_amount ASC, id) AS claimed_amount_dense_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_type)) AS distinct_status_in_claim_type,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        LAG(reported_at) OVER (PARTITION BY claim_type ORDER BY approved_amount, id) AS prev_reported_at,
        ROW_NUMBER() OVER w_claim_type_by_claim_date AS seq_no,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        LAG(claimed_amount, 3) OVER (PARTITION BY claim_type ORDER BY claim_date, id) AS prev3_claimed_amount,
        ROUND(CUME_DIST() OVER w_claim_type_by_approved_amoun, 3) AS approved_amount_cume_dist
    FROM filtered_claims
    WINDOW w_claim_type_by_claim_date AS (PARTITION BY claim_type ORDER BY claim_date, id), w_claim_type_by_approved_amoun AS (PARTITION BY claim_type ORDER BY approved_amount, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        claimed_amount_dense_rank,
        distinct_status_in_claim_type,
        approved_amount_cume_dist,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 0), 4) AS approved_amount_share_of_policy_id_adjuster_id,
        MAX(approved_amount) OVER (PARTITION BY distinct_status_in_claim_type ORDER BY claim_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_max_4,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no ORDER BY rows_in_claim_type NULLS LAST), 3) AS rows_in_claim_type_cume_dist,
        LAG(approved_amount_cume_dist) OVER (PARTITION BY distinct_status_in_claim_type ORDER BY claim_date NULLS LAST, id) AS prev_approved_amount_cume_dist,
        COUNT(*) OVER (PARTITION BY policy_id, adjuster_id) AS rows_in_policy_id_adjuster_id,
        LEAD(approved_amount_cume_dist, 3) OVER (PARTITION BY distinct_status_in_claim_type ORDER BY claim_date NULLS LAST, id) AS next3_approved_amount_cume_dist
    FROM sequenced
)
-- Final projection
SELECT
    id,
    claim_date,
    status,
    claimed_amount_dense_rank,
    approved_amount_share_of_policy_id_adjuster_id,
    approved_amount_max_4,
    rows_in_claim_type_cume_dist,
    prev_approved_amount_cume_dist,
    rows_in_policy_id_adjuster_id,
    next3_approved_amount_cume_dist,
    LOWER(TRIM(status)) AS status_clean
FROM with_windows
WHERE approved_amount_cume_dist < 0.8
ORDER BY approved_amount_max_4 DESC, rows_in_claim_type_cume_dist, id
LIMIT 20

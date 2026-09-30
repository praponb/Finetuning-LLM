WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        CASE WHEN status IN ('APPROVED', 'PAID') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        CASE WHEN claim_type IN ('medical', 'delay') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-02-12'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        approved_amount_bucket - LAG(approved_amount_bucket) OVER w_adjuster_id_by_claim_date AS approved_amount_bucket_change,
        RANK() OVER w_all_by_claim_date AS claim_date_rank,
        MIN(approved_amount_bucket) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_approved_amount_bucket
    FROM prepared
    WINDOW w_adjuster_id_by_claim_date AS (PARTITION BY adjuster_id ORDER BY claim_date DESC, id), w_all_by_claim_date AS (ORDER BY claim_date DESC NULLS FIRST, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        claim_type_group,
        approved_amount_bucket_change,
        adjuster_id_min_approved_amount_bucket,
        COALESCE(claim_date_rank, 0) AS claim_date_rank_filled,
        COALESCE(claim_date_rank, 0) AS claim_date_rank_filled_2,
        ROUND(approved_amount_bucket / NULLIF(claimed_amount, 0), 3) AS approved_amount_bucket_per_claimed_amount
    FROM metrics
    WHERE adjuster_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY claim_type_group, adjuster_id ORDER BY approved_amount_bucket_change ASC NULLS LAST) AS approved_amount_bucket_change_dense_rank,
        LAG(claim_date_rank_filled_2) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount, id) AS prev_claim_date_rank_filled_2,
        SIZE(COLLECT_SET(claim_type_group) OVER (PARTITION BY adjuster_id)) AS distinct_claim_type_group_in_adjuster_id,
        LAG(claim_date_rank_filled_2) OVER (PARTITION BY policy_id ORDER BY claim_date_rank_filled_2 DESC, id) AS prev_claim_date_rank_filled_2_2,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        FIRST_VALUE(claim_date_rank_filled_2) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_rank_filled_2
    FROM derived
),
summary AS (
    -- Summarise per group
    SELECT
        policy_id,
        first_claim_date_rank_filled_2,
        adjuster_id,
        COUNT(*) AS cnt,
        ROUND(STDDEV(distinct_claim_type_group_in_adjuster_id), 2) AS stddev_distinct_claim_type_group_in_adjuster_id,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MIN(claim_date_rank_filled_2) AS min_claim_date_rank_filled_2,
        SUM(claimed_amount) AS total_claimed_amount
    FROM sequenced
    GROUP BY 1, 2, 3
)
-- Subtotals with CUBE
SELECT
    distinct_ids,
    min_claim_date_rank_filled_2,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
FROM summary
GROUP BY CUBE(distinct_ids, min_claim_date_rank_filled_2)
ORDER BY distinct_ids NULLS LAST, min_claim_date_rank_filled_2 NULLS LAST

WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_type_prefix,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status), 2) AS status_avg_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, adjuster_id), 0), 4) AS claimed_amount_share_of_status_adjuster_id,
        ROUND(CUME_DIST() OVER (PARTITION BY status, adjuster_id ORDER BY approved_amount DESC), 3) AS approved_amount_cume_dist,
        ROUND(AVG(claimed_amount) OVER w_status_adjuster_id, 2) AS status_adjuster_id_avg_claimed_amount,
        RANK() OVER (PARTITION BY policy_id ORDER BY claimed_amount) AS claimed_amount_rank,
        LEAD(approved_amount) OVER (PARTITION BY YEAR(reported_at) ORDER BY claimed_amount DESC, id) AS next_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status, adjuster_id)) AS distinct_policy_id_in_status_adjuster_id,
        RANK() OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS claimed_amount_rank_2,
        RANK() OVER (ORDER BY approved_amount, id) AS approved_amount_rank,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY status, policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS approved_amount_max_3_prior,
        LAG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY approved_amount, id) AS prev_approved_amount,
        ROUND(PERCENT_RANK() OVER w_status_policy_id_by_approved_2, 3) AS approved_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER w_reported_at_year_by_claimed_2, 3) AS claimed_amount_percent_rank
    FROM recent_claims
    WINDOW w_status_adjuster_id AS (PARTITION BY status, adjuster_id), w_status_policy_id_by_approved_2 AS (PARTITION BY status, policy_id ORDER BY approved_amount, id), w_reported_at_year_by_claimed_2 AS (PARTITION BY YEAR(reported_at) ORDER BY claimed_amount DESC)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_adjuster_id_approved_amount AS rows_in_adjuster_id_approved_amount_rank,
        SIZE(COLLECT_SET(status) OVER w_adjuster_id_approved_amount) AS distinct_status_in_adjuster_id_approved_amount_r,
        ROW_NUMBER() OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount_rank NULLS LAST, id) AS claimed_amount_rank_seq,
        ROUND(PERCENT_RANK() OVER w_all_by_last_known_claimed_am, 3) AS last_known_claimed_amount_percent_rank,
        DENSE_RANK() OVER w_all_by_claimed_amount_avg_3 AS claimed_amount_avg_3_dense_rank
    FROM sequenced
    WINDOW w_adjuster_id_approved_amount AS (PARTITION BY adjuster_id, approved_amount_rank), w_all_by_last_known_claimed_am AS (ORDER BY last_known_claimed_amount DESC, id), w_all_by_claimed_amount_avg_3 AS (ORDER BY claimed_amount_avg_3)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    status,
    reported_at,
    claim_type_prefix,
    claimed_amount_rank,
    next_approved_amount,
    distinct_policy_id_in_status_adjuster_id,
    claimed_amount_rank_2,
    approved_amount_rank,
    last_known_claimed_amount,
    claimed_amount_avg_3,
    approved_amount_max_3_prior,
    approved_amount_percent_rank,
    rows_in_adjuster_id_approved_amount_rank,
    distinct_status_in_adjuster_id_approved_amount_r,
    last_known_claimed_amount_percent_rank,
    claimed_amount_avg_3_dense_rank,
    TO_DATE(reported_at) AS reported_at_date,
    LENGTH(TRIM(status)) AS status_len
FROM ranked
ORDER BY claimed_amount DESC, id DESC

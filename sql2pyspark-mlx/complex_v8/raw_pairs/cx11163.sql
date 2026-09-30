WITH filtered_claims AS (
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
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE adjuster_id >= 7
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3,
        LEAD(approved_amount_bucket) OVER (PARTITION BY policy_id, status ORDER BY claim_date NULLS LAST, id) AS next_approved_amount_bucket,
        ROUND(approved_amount_bucket - AVG(approved_amount_bucket) OVER (PARTITION BY status, claim_type), 2) AS approved_amount_bucket_vs_status_claim_type_avg,
        MAX(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_max_2_prior,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, status ORDER BY claim_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        ROUND(AVG(claimed_amount) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7_2,
        ROW_NUMBER() OVER (ORDER BY reported_at, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_policy_id_status_by_claim_da_3, 3) AS claim_date_percent_rank,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, status ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        LEAD(claimed_amount, 3) OVER (ORDER BY reported_at, id) AS next3_claimed_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, status ORDER BY claim_date NULLS LAST, id), 3) AS claim_date_cume_dist,
        MIN(approved_amount_bucket) OVER (PARTITION BY status, claim_type) AS status_claim_type_min_approved_amount_bucket,
        SUM(approved_amount_bucket) OVER (PARTITION BY claim_type, status ORDER BY approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_bucket_sum_6_prior,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, claim_type)) AS distinct_adjuster_id_in_status_claim_type
    FROM filtered_claims
    WINDOW w_policy_id_status_by_claim_da_3 AS (PARTITION BY policy_id, status ORDER BY claim_date NULLS LAST)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek
    FROM windowed
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY next_approved_amount_bucket ASC NULLS LAST), 3) AS next_approved_amount_bucket_cume_dist,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY claim_date_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        DENSE_RANK() OVER (ORDER BY claim_date_percent_rank DESC, id) AS claim_date_percent_rank_dense_rank,
        DENSE_RANK() OVER w_status_by_next_approved_amou AS next_approved_amount_bucket_dense_rank,
        SUM(claimed_amount) OVER (PARTITION BY status ORDER BY next_approved_amount_bucket ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_3,
        LAST_VALUE(next3_claimed_amount) OVER (PARTITION BY status ORDER BY next_approved_amount_bucket ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next3_claimed_amount,
        SUM(next3_claimed_amount) OVER (PARTITION BY status ORDER BY next_approved_amount_bucket ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next3_claimed_amount_sum_3,
        SIZE(COLLECT_SET(status) OVER w_policy_id) AS distinct_status_in_policy_id
    FROM labeled
    WINDOW w_status_by_next_approved_amou AS (PARTITION BY status ORDER BY next_approved_amount_bucket ASC NULLS LAST), w_policy_id AS (PARTITION BY policy_id)
),
totals AS (
    -- Summarise per group
    SELECT
        reported_at_dayofweek,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT status) AS distinct_status,
        ROUND(SUM(claim_date_percent_rank_dense_rank) / NULLIF(SUM(claimed_amount_sum_3), 0), 3) AS claim_date_percent_rank_dense_rank_to_claimed_am
    FROM with_trends
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
SELECT
    reported_at_dayofweek,
    n_rows,
    distinct_status,
    claim_date_percent_rank_dense_rank_to_claimed_am,
    GREATEST(reported_at_dayofweek, n_rows) AS greatest_reported_at_dayofweek_n_rows,
    GREATEST(reported_at_dayofweek, distinct_status) AS greatest_reported_at_dayofweek_distinct_status
FROM totals
ORDER BY distinct_status

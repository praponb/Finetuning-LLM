WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE (status = 'REJECTED' OR claim_type <> 'delay')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        status_len,
        NTILE(4) OVER w_policy_id_by_reported_at AS reported_at_quartile,
        ROUND(status_len / NULLIF(SUM(status_len) OVER (PARTITION BY claim_type), 0), 4) AS status_len_share_of_claim_type,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY claim_type), 2) AS status_len_vs_claim_type_avg,
        NTILE(10) OVER (ORDER BY claim_date, id) AS claim_date_decile,
        NTILE(5) OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS reported_at_quintile,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY claim_date, id))) / 60, 1) AS minutes_since_prev_reported_at,
        MAX(status_len) OVER (PARTITION BY policy_id) AS policy_id_max_status_len,
        ROUND(AVG(status_len) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_status_len,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date), 3) AS claim_date_percent_rank,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        MAX(status_len) OVER (ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_len_max_6_prior,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY status_len, id) AS seq_no_2
    FROM scoped_claims
    WINDOW w_policy_id_by_reported_at AS (PARTITION BY policy_id ORDER BY reported_at, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE seq_no_2 <= 5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_distinct_adjuster_id_in_clai AS status_len_share_of_claim_type_dense_rank,
        NTH_VALUE(last_known_reported_at, 2) OVER (PARTITION BY distinct_adjuster_id_in_claim_type, status_len ORDER BY status_len_share_of_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_reported_at,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY distinct_adjuster_id_in_claim_type, status_len ORDER BY status_len_share_of_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        SUM(seq_no) OVER w_claim_date_month_distinct_ad AS claim_date_month_distinct_adjuster_id_in_claim_t
    FROM top_per_group
    WINDOW w_distinct_adjuster_id_in_clai AS (PARTITION BY distinct_adjuster_id_in_claim_type, status_len ORDER BY status_len_share_of_claim_type DESC), w_claim_date_month_distinct_ad AS (PARTITION BY MONTH(claim_date), distinct_adjuster_id_in_claim_type)
)
SELECT
    id,
    adjuster_id,
    claim_date,
    status_len,
    reported_at_quartile,
    status_len_share_of_claim_type,
    status_len_vs_claim_type_avg,
    reported_at_quintile,
    seq_no,
    minutes_since_prev_reported_at,
    claim_type_avg_status_len,
    distinct_adjuster_id_in_policy_id,
    distinct_adjuster_id_in_claim_type,
    status_len_max_6_prior,
    status_len_share_of_claim_type_dense_rank,
    second_last_known_reported_at,
    first_known_claim_date,
    claim_date_month_distinct_adjuster_id_in_claim_t
FROM with_windows
ORDER BY reported_at_quintile DESC, status_len_share_of_claim_type_dense_rank DESC, id
LIMIT 10

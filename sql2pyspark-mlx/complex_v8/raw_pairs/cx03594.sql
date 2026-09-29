WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.status_slug,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.start_date >= DATE '2024-06-19'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(policy_start_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_id ORDER BY policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_start_date,
        LEAD(policy_start_date) OVER (PARTITION BY adjuster_id, policy_id ORDER BY policy_start_date, id) AS next_policy_start_date,
        ROW_NUMBER() OVER w_adjuster_id_policy_id_by_pol_2 AS seq_no,
        LEAD(policy_start_date, 3) OVER (PARTITION BY claim_type, policy_id ORDER BY policy_start_date NULLS LAST, id) AS next3_policy_start_date,
        ROUND(CUME_DIST() OVER (ORDER BY policy_start_date NULLS LAST, id), 3) AS policy_start_date_cume_dist,
        LAST_VALUE(policy_start_date) IGNORE NULLS OVER (PARTITION BY claim_type, policy_id ORDER BY policy_start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_policy_start_date,
        DENSE_RANK() OVER w_claim_type_by_policy_start_d AS policy_start_date_dense_rank,
        RANK() OVER w_claim_type_by_policy_start_d AS policy_start_date_rank
    FROM combined_details
    WINDOW w_adjuster_id_policy_id_by_pol_2 AS (PARTITION BY adjuster_id, policy_id ORDER BY policy_start_date, id), w_claim_type_by_policy_start_d AS (PARTITION BY claim_type ORDER BY policy_start_date ASC NULLS LAST)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        status_slug,
        policy_start_date,
        policy_holder_id,
        first_known_policy_start_date,
        next_policy_start_date,
        seq_no,
        next3_policy_start_date,
        policy_start_date_cume_dist,
        last_known_policy_start_date,
        policy_start_date_dense_rank
    FROM with_trends
    WHERE policy_start_date_rank <= 5
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status_slug,
        policy_start_date,
        policy_holder_id,
        first_known_policy_start_date,
        next_policy_start_date,
        seq_no,
        policy_start_date_cume_dist,
        last_known_policy_start_date,
        policy_start_date_dense_rank,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_start_date_dense_rank, policy_id)) AS distinct_claim_type_in_policy_start_date_dense_r,
        NTILE(3) OVER (PARTITION BY claim_type, MONTH(first_known_policy_start_date) ORDER BY next3_policy_start_date DESC, id) AS next3_policy_start_date_tercile,
        FIRST_VALUE(next3_policy_start_date) OVER (ORDER BY next3_policy_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next3_policy_start_date,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status, policy_holder_id)) AS distinct_claim_type_in_status_policy_holder_id,
        ROUND(policy_start_date_dense_rank - AVG(policy_start_date_dense_rank) OVER (PARTITION BY status, policy_holder_id), 2) AS policy_start_date_dense_rank_vs_status_policy_ho,
        ROUND(CUME_DIST() OVER (ORDER BY next3_policy_start_date ASC), 3) AS next3_policy_start_date_cume_dist
    FROM top_rows
),
summary AS (
    -- Summarise per group
    SELECT
        claim_type,
        DATE_TRUNC('MONTH', policy_start_date) AS policy_start_date_month,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MIN(policy_start_date_dense_rank_vs_status_policy_ho) AS min_policy_start_date_dense_rank_vs_status_polic,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM windowed
    GROUP BY claim_type, DATE_TRUNC('MONTH', policy_start_date)
)
SELECT
    claim_type,
    policy_start_date_month,
    cnt,
    distinct_policy_ids,
    distinct_adjuster_ids,
    min_policy_start_date_dense_rank_vs_status_polic,
    distinct_claim_types,
    SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
    CONCAT(claim_type, '#', CAST(distinct_claim_types AS STRING)) AS claim_type_ref
FROM summary
ORDER BY claim_type_prefix

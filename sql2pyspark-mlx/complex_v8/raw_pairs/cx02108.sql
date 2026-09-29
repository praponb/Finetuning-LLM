WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claim_type = 'medical'
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.reported_at,
        s.claim_type_status_label,
        a.adjuster_name
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        claim_type_status_label,
        adjuster_name,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_policy_id_status_by_reported) AS days_since_prev_claim_date,
        LEAD(reported_at) OVER w_policy_id_status_by_reported AS next_reported_at,
        LEAD(claim_date) OVER (PARTITION BY policy_id, status ORDER BY reported_at ASC, id) AS next_claim_date,
        DENSE_RANK() OVER (PARTITION BY policy_id, status ORDER BY reported_at ASC) AS reported_at_dense_rank
    FROM joined_adjuster
    WINDOW w_policy_id_status_by_reported AS (PARTITION BY policy_id, status ORDER BY reported_at ASC, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(claim_type_status_label)) AS claim_type_status_label_clean
    FROM ranked
    WHERE LOWER(adjuster_name) LIKE '%wa%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY reported_at_dense_rank, policy_id ORDER BY reported_at_dense_rank, id) AS reported_at_dense_rank_decile,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', next_reported_at), status ORDER BY next_claim_date, id) AS next_claim_date_rank,
        ROW_NUMBER() OVER (PARTITION BY reported_at_dense_rank, policy_id ORDER BY reported_at_dense_rank, id) AS rn,
        ROUND(reported_at_dense_rank - AVG(reported_at_dense_rank) OVER (PARTITION BY reported_at_dense_rank), 2) AS reported_at_dense_rank_vs_reported_at_dense_rank,
        NTH_VALUE(reported_at_dense_rank, 2) OVER (PARTITION BY status, DATE_TRUNC('MONTH', next_claim_date) ORDER BY days_since_prev_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_dense_rank
    FROM enriched
),
adjuster_id_stats AS (
    -- Max reported_at_dense_rank_vs_reported_at_dense_rank per adjuster_id
    SELECT adjuster_id, MAX(reported_at_dense_rank_vs_reported_at_dense_rank) AS max_reported_at_dense_rank_vs_reported_at_dense_
    FROM with_windows
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_reported_at_dense_rank_vs_reported_at_dense_,
        ROUND(s.reported_at_dense_rank_vs_reported_at_dense_rank - g.max_reported_at_dense_rank_vs_reported_at_dense_, 2) AS reported_at_dense_rank_vs_reported_at_dense_rank_2
    FROM with_windows s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.reported_at_dense_rank_vs_reported_at_dense_rank = g.max_reported_at_dense_rank_vs_reported_at_dense_
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    reported_at,
    claim_type_status_label,
    adjuster_name,
    days_since_prev_claim_date,
    next_reported_at,
    next_claim_date,
    reported_at_dense_rank,
    claim_type_status_label_clean,
    reported_at_dense_rank_decile,
    next_claim_date_rank,
    rn,
    second_reported_at_dense_rank,
    max_reported_at_dense_rank_vs_reported_at_dense_,
    reported_at_dense_rank_vs_reported_at_dense_rank_2
FROM above_average
ORDER BY reported_at_dense_rank_decile DESC, policy_id DESC NULLS FIRST, id
LIMIT 10

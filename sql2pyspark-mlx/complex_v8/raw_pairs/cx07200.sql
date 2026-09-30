WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE approved_amount BETWEEN 492.6 AND 21750.2
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        HOUR(reported_at) AS reported_at_hour,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM recent_claims
    WHERE claim_type_ref IS NOT NULL
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type = 'collision') AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM tagged s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(approved_amount - AVG(approved_amount) OVER w_claim_type_adjuster_id, 2) AS approved_amount_vs_claim_type_adjuster_id_avg,
        ROW_NUMBER() OVER w_claim_type_by_reported_at_ho AS seq_no,
        DENSE_RANK() OVER w_all_by_reported_at_hour_ties AS reported_at_hour_dense_rank,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY reported_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        RANK() OVER (PARTITION BY claim_type ORDER BY reported_at_hour) AS reported_at_hour_rank,
        ROW_NUMBER() OVER (ORDER BY reported_at_hour ASC, id) AS seq_no_2
    FROM with_counts
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id), w_claim_type_by_reported_at_ho AS (PARTITION BY claim_type ORDER BY reported_at_hour, id), w_all_by_reported_at_hour_ties AS (ORDER BY reported_at_hour ASC)
),
unique_rows AS (
    -- Keep the latest row per claim_type, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_type_ref,
        reported_at_hour,
        status_ref,
        claim_claim_type_count,
        approved_amount_vs_claim_type_adjuster_id_avg,
        seq_no,
        reported_at_hour_dense_rank,
        last_known_approved_amount,
        reported_at_hour_rank,
        seq_no_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY reported_at DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
)
SELECT id, measure, metric_value
FROM (
    SELECT id, seq_no_2, claim_claim_type_count, seq_no, reported_at_hour_rank
    FROM unique_rows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (seq_no_2, claim_claim_type_count, seq_no, reported_at_hour_rank)
) unp
ORDER BY id, measure

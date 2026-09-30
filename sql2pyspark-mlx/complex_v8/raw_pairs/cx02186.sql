WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id <= 4
      AND claimed_amount BETWEEN 11235.6 AND 40639.7
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount < 31752.9
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_date,
        claim_type,
        reported_at,
        ROUND(PERCENT_RANK() OVER w_status_by_claim_date, 3) AS claim_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY claim_date) AS claim_date_dense_rank
    FROM without_matches
    WINDOW w_status_by_claim_date AS (PARTITION BY status ORDER BY claim_date DESC, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(claim_date) AS claim_date_year,
        ROUND(SUM(claim_date_percent_rank) / NULLIF(SUM(seq_no), 0), 3) AS claim_date_percent_rank_to_seq_no_ratio,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE claim_type = 'delay') AS n_claim_type_delay,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(claim_date_dense_rank) AS total_claim_date_dense_rank
    FROM metrics
    GROUP BY 1
    HAVING SUM(claim_date_dense_rank) > 3
),
annotated AS (
    -- Derive helper columns
    SELECT
        claim_date_year,
        claim_date_percent_rank_to_seq_no_ratio,
        distinct_ids,
        n_claim_type_delay,
        claim_type_list,
        total_claim_date_dense_rank,
        CASE WHEN distinct_ids >= 7 THEN 'large' WHEN distinct_ids >= 3 THEN 'medium' ELSE 'small' END AS distinct_ids_tier
    FROM summary
    WHERE total_claim_date_dense_rank <= 45
)
SELECT claim_date_year, field, metric_value
FROM (
    SELECT claim_date_year, n_claim_type_delay, distinct_ids
    FROM annotated
) src
UNPIVOT (
    metric_value FOR field IN (n_claim_type_delay, distinct_ids)
) unp
ORDER BY claim_date_year, field

WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE (claimed_amount < 43500.4 OR adjuster_id >= 5)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        CONCAT_WS('-', status, claim_type_slug) AS status_claim_type_slug_label
    FROM scoped_claims
    WHERE claim_date BETWEEN DATE '2024-04-15' AND DATE '2025-04-14'
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount > 3703.0
        )
),
per_group AS (
    -- Group-level totals
    SELECT
        status,
        adjuster_id,
        policy_id,
        COUNT(DISTINCT status) AS distinct_status,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE status = 'PAID') AS n_status_paid
    FROM filtered
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(n_rows) OVER (PARTITION BY status, distinct_status ORDER BY distinct_status DESC NULLS FIRST, status, adjuster_id, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n_rows,
        DENSE_RANK() OVER w_status_distinct_status_by_di_2 AS distinct_status_dense_rank,
        ROUND(n_status_paid - AVG(n_status_paid) OVER (PARTITION BY adjuster_id), 2) AS n_status_paid_vs_adjuster_id_avg,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY adjuster_id), 0), 4) AS n_rows_share_of_adjuster_id,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows, adjuster_id ORDER BY n_status_paid), 3) AS n_status_paid_cume_dist
    FROM per_group
    WINDOW w_status_distinct_status_by_di_2 AS (PARTITION BY status, distinct_status ORDER BY distinct_status DESC NULLS FIRST)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM windowed s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_adjuster_id_by_n_status_paid AS n_status_paid_vs_adjuster_id_avg_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY n_status_paid_vs_adjuster_id_avg ASC, status, adjuster_id, policy_id), 3) AS n_status_paid_vs_adjuster_id_avg_cume_dist
    FROM with_stats
    WINDOW w_adjuster_id_by_n_status_paid AS (PARTITION BY adjuster_id ORDER BY n_status_paid_vs_adjuster_id_avg ASC)
)
SELECT status, adjuster_id, metric_name, value
FROM (
    SELECT status, adjuster_id, n_status_paid_vs_adjuster_id_avg_dense_rank, n_rows, last_n_rows, n_status_paid
    FROM with_trends
) src
UNPIVOT (
    value FOR metric_name IN (n_status_paid_vs_adjuster_id_avg_dense_rank, n_rows, last_n_rows, n_status_paid)
) unp

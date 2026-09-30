WITH filtered AS (
    -- Scope the claims rows
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
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount > 1273.7
      AND policy_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status IS NOT NULL
        )
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        COALESCE(approved_amount_filled, 0.0) AS approved_amount_filled_filled
    FROM qualified
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        status,
        adjuster_id,
        COUNT(*) AS row_count,
        ROUND(STDDEV(claim_type_len), 2) AS stddev_claim_type_len,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE claim_type IN ('flood', 'collision')) AS n_claim_type_in_flood_collisio
    FROM scored
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM'), status, adjuster_id
),
qualified_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM grouped s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        claim_date_ym,
        status,
        adjuster_id,
        row_count,
        distinct_ids,
        n_claim_type_in_flood_collisio,
        LAST_VALUE(row_count) IGNORE NULLS OVER (ORDER BY stddev_claim_type_len, claim_date_ym, status, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_count,
        distinct_ids - LAG(distinct_ids) OVER w_all_by_stddev_claim_type_len_2 AS distinct_ids_change,
        SUM(row_count) OVER (ORDER BY stddev_claim_type_len, claim_date_ym, status, adjuster_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_count_sum_6_prior,
        DENSE_RANK() OVER w_all_by_stddev_claim_type_len_4 AS stddev_claim_type_len_dense_rank,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY n_claim_type_in_flood_collisio, distinct_ids), 2) AS row_count_vs_n_claim_type_in_flood_collisio_dist
    FROM qualified_2
    WINDOW w_all_by_stddev_claim_type_len_2 AS (ORDER BY stddev_claim_type_len, claim_date_ym, status, adjuster_id), w_all_by_stddev_claim_type_len_4 AS (ORDER BY stddev_claim_type_len)
)
SELECT
    claim_date_ym,
    status,
    row_count,
    distinct_ids,
    n_claim_type_in_flood_collisio,
    last_known_row_count,
    distinct_ids_change,
    row_count_sum_6_prior,
    stddev_claim_type_len_dense_rank
FROM with_windows
ORDER BY distinct_ids_change, last_known_row_count NULLS LAST

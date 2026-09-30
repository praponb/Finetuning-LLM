WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
    FROM claims
    WHERE policy_id <= 9
      AND status = 'PAID'
),
stats AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        policy_id,
        SUM(claimed_amount) AS total_claimed_amount,
        MIN(reported_at) AS first_reported_at,
        MAX(reported_at) AS last_reported_at,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned
    WHERE claim_type_ref IS NOT NULL
    GROUP BY adjuster_id, policy_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE total_claimed_amount > (SELECT AVG(total_claimed_amount) FROM stats)
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM screened s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_reported_at, 2) OVER (PARTITION BY n_rows, YEAR(last_reported_at) ORDER BY stddev_claimed_amount, adjuster_id, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_reported_at,
        ROUND(CUME_DIST() OVER w_n_rows_last_reported_at_year_2, 3) AS stddev_claimed_amount_cume_dist,
        ROUND(stddev_claimed_amount / NULLIF(SUM(stddev_claimed_amount) OVER (PARTITION BY n_rows), 0), 4) AS stddev_claimed_amount_share_of_n_rows,
        RANK() OVER (PARTITION BY distinct_ids ORDER BY distinct_ids DESC, adjuster_id, policy_id) AS distinct_ids_rank,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER (PARTITION BY n_rows), 0), 4) AS distinct_ids_share_of_n_rows,
        MAX(stddev_claimed_amount) OVER (PARTITION BY n_rows, YEAR(last_reported_at) ORDER BY stddev_claimed_amount, adjuster_id, policy_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS stddev_claimed_amount_max_3_prior
    FROM with_activity
    WINDOW w_n_rows_last_reported_at_year_2 AS (PARTITION BY n_rows, YEAR(last_reported_at) ORDER BY stddev_claimed_amount)
)
-- Final projection
SELECT
    adjuster_id,
    policy_id,
    total_claimed_amount,
    first_reported_at,
    last_reported_at,
    stddev_claimed_amount,
    distinct_ids,
    second_first_reported_at,
    stddev_claimed_amount_share_of_n_rows,
    distinct_ids_rank,
    distinct_ids_share_of_n_rows,
    ROUND(n_rows / NULLIF(distinct_ids_rank, 0), 3) AS n_rows_per_distinct_ids_rank
FROM with_windows
ORDER BY second_first_reported_at

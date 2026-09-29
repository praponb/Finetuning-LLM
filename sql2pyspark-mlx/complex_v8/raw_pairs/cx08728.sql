WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date >= DATE '2025-01-05'
      AND end_date >= DATE '2026-01-24'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        end_date,
        status,
        product_ref,
        status_ref,
        ROUND(CUME_DIST() OVER w_all_by_start_date, 3) AS start_date_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY end_date DESC, id), 3) AS end_date_cume_dist,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        LAG(start_date, 2) OVER (PARTITION BY status, holder_id ORDER BY end_date ASC NULLS LAST, id) AS prev2_start_date,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_2,
        DENSE_RANK() OVER (ORDER BY start_date) AS start_date_dense_rank,
        DENSE_RANK() OVER (PARTITION BY status, holder_id ORDER BY end_date ASC NULLS LAST, id) AS end_date_dense_rank,
        ROW_NUMBER() OVER w_all_by_end_date AS rn,
        RANK() OVER (ORDER BY end_date DESC) AS end_date_rank
    FROM prepared_policys
    WINDOW w_all_by_start_date AS (ORDER BY start_date, id), w_all_by_end_date AS (ORDER BY end_date DESC, id)
),
derived AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM sequenced
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(end_date_cume_dist) OVER (PARTITION BY MONTH(second_start_date), DATE_TRUNC('MONTH', end_date) ORDER BY last_known_end_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS end_date_cume_dist_max_3_prior,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(prev2_start_date))) AS distinct_status_in_prev2_start_date_month,
        COUNT(*) OVER w_prev2_start_date_month AS rows_in_prev2_start_date_month,
        SUM(end_date_rank) OVER (PARTITION BY end_date_dense_rank, status ORDER BY second_start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_end_date_rank
    FROM derived
    WINDOW w_prev2_start_date_month AS (PARTITION BY MONTH(prev2_start_date))
)
-- Final projection
SELECT
    id,
    start_date,
    end_date,
    product_ref,
    status_ref,
    start_date_cume_dist,
    end_date_cume_dist,
    last_known_end_date,
    prev2_start_date,
    second_start_date,
    last_known_end_date_2,
    start_date_dense_rank,
    rn,
    status_prefix,
    end_date_cume_dist_max_3_prior,
    distinct_status_in_prev2_start_date_month,
    rows_in_prev2_start_date_month
FROM with_trends
ORDER BY end_date_cume_dist

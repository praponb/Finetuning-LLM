WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'AUTO'
      AND holder_id >= 2
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM scoped_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium >= 1042.5
              AND p.end_date >= DATE '2025-10-12'
        )
      AND s.end_date >= DATE '2026-01-23'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(end_date) OVER (ORDER BY end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LAG(start_date) OVER w_all_by_end_date AS prev_start_date,
        ROUND(CUME_DIST() OVER w_all_by_end_date, 3) AS end_date_cume_dist,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date
    FROM qualified
    WINDOW w_all_by_end_date AS (ORDER BY end_date NULLS LAST, id)
),
totals AS (
    -- Group-level totals
    SELECT
        product,
        DATE_TRUNC('MONTH', prev_start_date) AS prev_start_date_month,
        holder_id,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        SUM(end_date_cume_dist) AS total_end_date_cume_dist,
        COUNT(*) AS row_count,
        DATEDIFF(MAX(prev_start_date), MIN(prev_start_date)) AS prev_start_date_span_days
    FROM metrics
    WHERE end_date_cume_dist < 0.8
    GROUP BY product, DATE_TRUNC('MONTH', prev_start_date), holder_id
    HAVING AVG(end_date_cume_dist) > 0.1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        product,
        prev_start_date_month,
        holder_id,
        start_date_span_days,
        total_end_date_cume_dist,
        row_count,
        LEAD(total_end_date_cume_dist, 2) OVER (ORDER BY row_count ASC NULLS LAST, product, prev_start_date_month, holder_id) AS next2_total_end_date_cume_dist,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY row_count), 2) AS row_count_vs_row_count_avg
    FROM totals
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(total_end_date_cume_dist) OVER (PARTITION BY holder_id) AS holder_id_max_total_end_date_cume_dist,
        NTILE(5) OVER w_all_by_start_date_span_days AS start_date_span_days_quintile,
        MAX(row_count) OVER (ORDER BY start_date_span_days, product, prev_start_date_month, holder_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_count_max_2_prior,
        ROUND(row_count_vs_row_count_avg / NULLIF(SUM(row_count_vs_row_count_avg) OVER w_holder_id, 0), 4) AS row_count_vs_row_count_avg_share_of_holder_id,
        NTH_VALUE(prev_start_date_month, 2) OVER (ORDER BY start_date_span_days, product, prev_start_date_month, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_start_date_month,
        LAST_VALUE(start_date_span_days) IGNORE NULLS OVER (ORDER BY start_date_span_days, product, prev_start_date_month, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date_span_days
    FROM sequenced
    WINDOW w_all_by_start_date_span_days AS (ORDER BY start_date_span_days, product, prev_start_date_month, holder_id), w_holder_id AS (PARTITION BY holder_id)
)
SELECT product, prev_start_date_month, field, amount
FROM (
    SELECT product, prev_start_date_month, next2_total_end_date_cume_dist, row_count_vs_row_count_avg_share_of_holder_id
    FROM ranked
) src
UNPIVOT (
    amount FOR field IN (next2_total_end_date_cume_dist, row_count_vs_row_count_avg_share_of_holder_id)
) unp

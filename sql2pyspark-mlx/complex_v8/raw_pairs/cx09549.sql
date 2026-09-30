WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE status IN ('LAPSED', 'ACTIVE')
      AND start_date BETWEEN DATE '2024-05-07' AND DATE '2025-07-07'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(end_date) OVER w_product_by_days_since_start AS prev_end_date,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY MONTH(end_date), status)) AS distinct_product_in_end_date_month_status,
        ROUND(PERCENT_RANK() OVER w_product_by_days_since_start_2, 3) AS days_since_start_date_percent_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY MONTH(end_date), status)) AS distinct_holder_id_in_end_date_month_status
    FROM scoped
    WINDOW w_product_by_days_since_start AS (PARTITION BY product ORDER BY days_since_start_date ASC, id), w_product_by_days_since_start_2 AS (PARTITION BY product ORDER BY days_since_start_date ASC)
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount <= 21750.2
              AND TO_DATE(c.reported_at) < DATE '2025-02-12'
        )
)
SELECT
    distinct_holder_id_in_end_date_month_status,
    distinct_product_in_end_date_month_status,
    GROUPING(distinct_holder_id_in_end_date_month_status) AS is_total_distinct_holder_id_in_end_date_month_st,
    MIN(prev_end_date) AS first_prev_end_date,
    PERCENTILE_APPROX(days_since_start_date_percent_rank, 0.5) AS median_days_since_start_date_percent_rank
FROM without_matches
GROUP BY GROUPING SETS ((distinct_holder_id_in_end_date_month_status, distinct_product_in_end_date_month_status), (distinct_holder_id_in_end_date_month_status), (distinct_product_in_end_date_month_status), ())
ORDER BY distinct_holder_id_in_end_date_month_status NULLS LAST, distinct_product_in_end_date_month_status NULLS LAST

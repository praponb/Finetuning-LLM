WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        UPPER(TRIM(status)) AS status_clean
    FROM policies
    WHERE end_date >= DATE '2026-02-16'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_holder_id_start_date_month AS rows_in_holder_id_start_date_month,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id)) AS distinct_product_in_holder_id,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        SUM(premium) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        SUM(premium) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS end_date_seq,
        LAG(end_date) OVER w_all_by_end_date AS prev_end_date,
        LEAD(start_date, 2) OVER w_status_holder_id_by_premium AS next2_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, MONTH(start_date)), 0), 4) AS premium_share_of_holder_id_start_date_month,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior
    FROM scoped
    WINDOW w_holder_id_start_date_month AS (PARTITION BY holder_id, MONTH(start_date)), w_all_by_end_date AS (ORDER BY end_date DESC, id), w_status_holder_id_by_premium AS (PARTITION BY status, holder_id ORDER BY premium DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    rows_in_holder_id_start_date_month,
    distinct_product_in_holder_id,
    premium_sum_3_prior,
    running_premium,
    premium_vs_holder_id_avg,
    end_date_seq,
    premium_share_of_holder_id_start_date_month,
    premium_avg_1_prior,
    CONCAT_WS(': ', status, product) AS status_product_label,
    CASE WHEN premium_sum_3_prior >= 9820.0 THEN 'A' WHEN premium_sum_3_prior >= 4640.0 THEN 'B' ELSE 'C' END AS premium_sum_3_prior_tier
FROM with_trends
ORDER BY holder_id DESC, rows_in_holder_id_start_date_month DESC, id
LIMIT 10

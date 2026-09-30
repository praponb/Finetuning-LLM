WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE premium >= 2289.0
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_days_since_start_date, 3) AS days_since_start_date_percent_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date))) AS distinct_holder_id_in_end_date_month,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS rows_in_end_date_month,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 2) AS days_since_start_date_vs_end_date_month_avg,
        ROUND(premium - AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 2) AS premium_vs_end_date_month_avg,
        days_since_start_date - LAG(days_since_start_date) OVER (ORDER BY days_since_start_date ASC, id) AS days_since_start_date_change,
        NTH_VALUE(premium, 2) OVER (ORDER BY days_since_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_2
    FROM valid
    WINDOW w_all_by_days_since_start_date AS (ORDER BY days_since_start_date ASC, id), w_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date))
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date,
        MONTH(start_date) AS start_date_month
    FROM ranked
    WHERE days_since_start_date_percent_rank > 0.3
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id)) AS distinct_status_in_holder_id,
        MAX(rows_in_end_date_month) OVER (PARTITION BY product ORDER BY rows_in_end_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rows_in_end_date_month_so_far,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        MAX(premium) OVER (PARTITION BY distinct_holder_id_in_end_date_month, status ORDER BY days_since_start_date_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product ORDER BY rows_in_end_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_3,
        RANK() OVER (PARTITION BY MONTH(end_date) ORDER BY days_since_start_date_percent_rank DESC) AS days_since_start_date_percent_rank_rank
    FROM enriched
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    status,
    end_date_label,
    days_since_start_date_percent_rank,
    rows_in_end_date_month,
    second_premium,
    days_since_start_date_vs_end_date_month_avg,
    premium_vs_end_date_month_avg,
    days_since_start_date_change,
    second_premium_2,
    days_since_end_date,
    start_date_month,
    distinct_status_in_holder_id,
    max_rows_in_end_date_month_so_far,
    rows_in_holder_id,
    max_premium_so_far,
    second_premium_3,
    days_since_start_date_percent_rank_rank
FROM with_trends
ORDER BY premium DESC NULLS FIRST, rows_in_end_date_month

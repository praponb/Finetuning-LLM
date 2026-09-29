WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 0.25, 2) AS premium_adj,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM policies
    WHERE YEAR(start_date) = 2025
      AND status IN ('ACTIVE', 'LAPSED')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, holder_id ORDER BY start_date ASC NULLS LAST, id), 3) AS start_date_percent_rank,
        RANK() OVER (PARTITION BY status, holder_id ORDER BY start_date ASC NULLS LAST) AS start_date_rank,
        MAX(premium) OVER (PARTITION BY status, DATE_TRUNC('MONTH', start_date)) AS status_start_date_month_max_premium,
        ROUND(premium_adj - AVG(premium_adj) OVER w_status_start_date_month, 2) AS premium_adj_vs_status_start_date_month_avg,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER (PARTITION BY holder_id), 0), 4) AS premium_adj_share_of_holder_id,
        ROUND(premium_adj - AVG(premium_adj) OVER (PARTITION BY holder_id), 2) AS premium_adj_vs_holder_id_avg,
        RANK() OVER w_start_date_month_by_end_date AS end_date_rank,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        ROUND(AVG(premium_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_adj_avg_3_prior,
        ROUND(AVG(premium_adj) OVER (PARTITION BY status, holder_id ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_adj_avg_7,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, DATE_TRUNC('MONTH', start_date)), 2) AS premium_vs_status_start_date_month_avg,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        MAX(premium_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_adj_max_3,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM scoped_policys
    WINDOW w_status_start_date_month AS (PARTITION BY status, DATE_TRUNC('MONTH', start_date)), w_start_date_month_by_end_date AS (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY end_date NULLS LAST)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY last_known_premium, id), 3) AS last_known_premium_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        MAX(premium_adj_avg_7) OVER w_start_date_rank AS start_date_rank_max_premium_adj_avg_7,
        ROUND(premium_adj_vs_holder_id_avg / NULLIF(SUM(premium_adj_vs_holder_id_avg) OVER (PARTITION BY start_date_rank), 0), 4) AS premium_adj_vs_holder_id_avg_share_of_start_date
    FROM metrics
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_start_date_rank AS (PARTITION BY start_date_rank)
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    holder_id_min_premium,
    status_start_date_month_max_premium,
    premium_adj_vs_status_start_date_month_avg,
    premium_adj_vs_holder_id_avg,
    holder_id_max_premium,
    premium_adj_avg_7,
    premium_vs_status_start_date_month_avg,
    premium_adj_max_3,
    last_known_premium,
    last_known_premium_percent_rank,
    premium_share_of_holder_id,
    start_date_rank_max_premium_adj_avg_7,
    premium_adj_vs_holder_id_avg_share_of_start_date,
    INITCAP(TRIM(status)) AS status_clean,
    DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label_2
FROM windowed

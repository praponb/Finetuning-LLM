WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM policies
    WHERE holder_id >= 2
      AND start_date BETWEEN DATE '2024-01-25' AND DATE '2025-10-13'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status_group,
        s.end_date_month,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        status_group,
        end_date_month,
        holder_name,
        MAX(holder_risk_score) OVER (PARTITION BY MONTH(end_date_month), holder_id) AS end_date_month_month_holder_id_max_holder_risk_s,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY status_group ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        MAX(holder_risk_score) OVER w_product_status_group AS product_status_group_max_holder_risk_score,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY status_group, holder_id), 2) AS status_group_holder_id_avg_holder_risk_score,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY end_date, id)) AS days_since_prev_end_date,
        COUNT(*) OVER w_end_date_month_month_holder AS rows_in_end_date_month_month_holder_id,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_group, product ORDER BY end_date_month DESC NULLS FIRST, id), 3) AS end_date_month_percent_rank,
        RANK() OVER (PARTITION BY holder_id ORDER BY end_date) AS end_date_rank,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY status_group, product ORDER BY end_date_month DESC NULLS FIRST, id)) AS days_since_prev_end_date_2,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date_month_ties, 3) AS end_date_month_percent_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY holder_risk_score NULLS LAST, id), 3) AS holder_risk_score_cume_dist
    FROM enriched_holder
    WINDOW w_product_status_group AS (PARTITION BY product, status_group), w_end_date_month_month_holder AS (PARTITION BY MONTH(end_date_month), holder_id), w_all_by_end_date_month_ties AS (ORDER BY end_date_month)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        status_group,
        end_date_month,
        end_date_month_month_holder_id_max_holder_risk_s,
        last_known_end_date,
        product_status_group_max_holder_risk_score,
        days_since_prev_end_date,
        rows_in_end_date_month_month_holder_id,
        last_known_end_date_2,
        end_date_month_percent_rank,
        end_date_month_percent_rank_2,
        holder_risk_score_cume_dist,
        ROUND(AVG(end_date_rank) OVER (PARTITION BY end_date_rank), 2) AS end_date_rank_avg_end_date_rank,
        ROUND(holder_risk_score_cume_dist / NULLIF(SUM(holder_risk_score_cume_dist) OVER (PARTITION BY end_date_rank), 0), 4) AS holder_risk_score_cume_dist_share_of_end_date_ra,
        FIRST_VALUE(days_since_prev_end_date) IGNORE NULLS OVER (PARTITION BY end_date_rank, holder_id ORDER BY end_date_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_prev_end_date,
        MIN(days_since_prev_end_date_2) OVER (PARTITION BY status_group, holder_id ORDER BY status_group_holder_id_avg_holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_prev_end_date_2_so_far,
        SIZE(COLLECT_SET(status_group) OVER (PARTITION BY end_date_rank)) AS distinct_status_group_in_end_date_rank
    FROM windowed
)
SELECT
    id,
    status_group,
    end_date_month,
    end_date_month_month_holder_id_max_holder_risk_s,
    last_known_end_date,
    days_since_prev_end_date,
    rows_in_end_date_month_month_holder_id,
    end_date_month_percent_rank,
    holder_risk_score_cume_dist_share_of_end_date_ra,
    first_known_days_since_prev_end_date,
    min_days_since_prev_end_date_2_so_far,
    distinct_status_group_in_end_date_rank,
    TO_DATE(end_date_month) AS end_date_month_date
FROM sequenced
ORDER BY first_known_days_since_prev_end_date DESC NULLS FIRST

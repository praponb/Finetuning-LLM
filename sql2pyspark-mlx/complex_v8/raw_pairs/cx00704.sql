WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE product IN ('TRAVEL', 'AUTO')
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(start_date), product) AS rows_in_start_date_month_product,
        SUM(holder_risk_score) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        LAST_VALUE(premium) OVER (PARTITION BY product, holder_id ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        LAG(premium) OVER (ORDER BY start_date ASC, id) AS prev_premium,
        NTH_VALUE(holder_risk_score, 2) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        DENSE_RANK() OVER (ORDER BY start_date, id) AS start_date_dense_rank,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY MONTH(start_date), product), 2) AS holder_risk_score_vs_start_date_month_product_av,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        NTILE(10) OVER (ORDER BY start_date ASC, id) AS start_date_decile,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        premium - LAG(premium) OVER w_all_by_start_date AS premium_change,
        MAX(premium) OVER w_start_date_month_product AS start_date_month_product_max_premium,
        ROUND(CUME_DIST() OVER w_all_by_start_date, 3) AS start_date_cume_dist,
        MAX(holder_risk_score) OVER (PARTITION BY product, holder_id ORDER BY holder_risk_score DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS holder_risk_score_max_4
    FROM enriched
    WINDOW w_all_by_start_date AS (ORDER BY start_date, id), w_start_date_month_product AS (PARTITION BY MONTH(start_date), product)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY start_date_dense_rank DESC NULLS FIRST) AS start_date_dense_rank_dense_rank,
        SUM(start_date_cume_dist) OVER (PARTITION BY start_date_dense_rank, start_date_decile) AS start_date_dense_rank_start_date_decile_sum_star,
        ROUND(prev_premium - AVG(prev_premium) OVER (PARTITION BY start_date_dense_rank, start_date_decile), 2) AS prev_premium_vs_start_date_dense_rank_start_date,
        MAX(start_date_cume_dist) OVER (PARTITION BY start_date_dense_rank, start_date_decile) AS start_date_dense_rank_start_date_decile_max_star,
        LEAD(premium, 1, 0) OVER w_all_by_start_date_dense_rank_2 AS next_premium,
        ROW_NUMBER() OVER w_all_by_start_date_dense_rank_2 AS seq_no
    FROM with_windows
    WINDOW w_all_by_start_date_dense_rank_2 AS (ORDER BY start_date_dense_rank DESC NULLS FIRST, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        status,
        DATE_FORMAT(second_start_date, 'yyyy-MM') AS second_start_date_ym,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(AVG(holder_risk_score_vs_start_date_month_product_av), 2) AS avg_holder_risk_score_vs_start_date_month_produc,
        SUM(CASE WHEN status = 'CANCELLED' THEN running_holder_risk_score ELSE 0 END) AS running_holder_risk_score_status_cancelled
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', start_date), status, DATE_FORMAT(second_start_date, 'yyyy-MM')
)
SELECT
    start_date_month,
    status,
    second_start_date_ym,
    distinct_holder_ids,
    avg_holder_risk_score_vs_start_date_month_produc,
    running_holder_risk_score_status_cancelled
FROM grouped
WHERE avg_holder_risk_score_vs_start_date_month_produc BETWEEN -0.9 AND 0.7
ORDER BY status, running_holder_risk_score_status_cancelled DESC NULLS FIRST

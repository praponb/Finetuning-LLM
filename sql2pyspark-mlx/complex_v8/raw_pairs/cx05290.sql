WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', product, status) AS product_status_label,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE product = 'LIFE'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_status_label,
        p.risk_score AS holder_risk_score
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(premium) OVER (ORDER BY holder_risk_score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_max_7,
        DENSE_RANK() OVER (ORDER BY holder_risk_score) AS holder_risk_score_dense_rank,
        NTH_VALUE(premium, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        NTILE(10) OVER (ORDER BY holder_risk_score, id) AS holder_risk_score_decile,
        SUM(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        MIN(holder_risk_score) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_holder_risk_score_so_far,
        NTH_VALUE(start_date, 2) OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY MONTH(end_date)), 0), 4) AS holder_risk_score_share_of_end_date_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY holder_risk_score), 3) AS holder_risk_score_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(end_date)), 0), 4) AS premium_share_of_end_date_year,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY MONTH(end_date)), 2) AS holder_risk_score_vs_end_date_month_avg,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY MONTH(end_date)), 2) AS end_date_month_avg_holder_risk_score,
        COUNT(*) OVER (PARTITION BY MONTH(end_date)) AS rows_in_end_date_month
    FROM combined_details
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        holder_risk_score,
        premium_max_7,
        holder_risk_score_dense_rank,
        second_premium,
        holder_risk_score_decile,
        running_holder_risk_score,
        min_holder_risk_score_so_far,
        second_start_date,
        holder_risk_score_share_of_end_date_month,
        holder_risk_score_vs_end_date_month_avg,
        end_date_month_avg_holder_risk_score,
        rows_in_end_date_month,
        ROUND(CUME_DIST() OVER w_all_by_end_date_ties, 3) AS end_date_cume_dist,
        LAST_VALUE(holder_risk_score) OVER (PARTITION BY product, YEAR(end_date) ORDER BY end_date_month_avg_holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_risk_score,
        ROUND(min_holder_risk_score_so_far - AVG(min_holder_risk_score_so_far) OVER (PARTITION BY holder_id, MONTH(end_date)), 2) AS min_holder_risk_score_so_far_vs_holder_id_end_da,
        COUNT(*) OVER (PARTITION BY holder_id, MONTH(end_date)) AS rows_in_holder_id_end_date_month
    FROM windowed
    WINDOW w_all_by_end_date_ties AS (ORDER BY end_date DESC)
)
SELECT
    id,
    holder_id,
    product,
    end_date,
    status,
    holder_risk_score,
    premium_max_7,
    holder_risk_score_decile,
    min_holder_risk_score_so_far,
    second_start_date,
    holder_risk_score_share_of_end_date_month,
    holder_risk_score_vs_end_date_month_avg,
    rows_in_end_date_month,
    end_date_cume_dist,
    min_holder_risk_score_so_far_vs_holder_id_end_da,
    rows_in_holder_id_end_date_month,
    INITCAP(TRIM(product)) AS product_clean
FROM sequenced

WITH recent_policys AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE product NOT IN ('AUTO', 'LIFE')
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        p.risk_score AS holder_risk_score,
        p.holder_name,
        p.region AS holder_region
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score >= 0.3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        end_date,
        holder_risk_score,
        holder_name,
        holder_region,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY holder_id), 2) AS holder_risk_score_vs_holder_id_avg,
        SUM(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        RANK() OVER w_holder_id_by_holder_risk_sco_2 AS holder_risk_score_rank,
        LEAD(holder_risk_score, 1, 0) OVER (PARTITION BY holder_region ORDER BY holder_risk_score DESC, id) AS next_holder_risk_score,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_id), 2) AS holder_id_avg_holder_risk_score,
        SUM(holder_risk_score) OVER (PARTITION BY product ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS holder_risk_score_sum_2,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_id), 0), 4) AS holder_risk_score_share_of_holder_id,
        LEAD(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY holder_risk_score DESC, id) AS next_holder_risk_score_2,
        MAX(holder_risk_score) OVER (PARTITION BY product ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_6_prior,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY holder_risk_score DESC, id)) AS days_since_prev_end_date,
        MAX(holder_risk_score) OVER (PARTITION BY holder_id) AS holder_id_max_holder_risk_score,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id
    FROM combined_details_holder
    WINDOW w_holder_id_by_holder_risk_sco_2 AS (PARTITION BY holder_id ORDER BY holder_risk_score DESC)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        product,
        end_date,
        holder_risk_score,
        holder_name,
        holder_region,
        holder_risk_score_vs_holder_id_avg,
        running_holder_risk_score,
        next_holder_risk_score,
        holder_id_avg_holder_risk_score,
        holder_risk_score_sum_2,
        holder_risk_score_share_of_holder_id,
        next_holder_risk_score_2,
        holder_risk_score_max_6_prior,
        days_since_prev_end_date,
        holder_id_max_holder_risk_score,
        rows_in_holder_id
    FROM with_windows
    WHERE holder_risk_score_rank = 1
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        COUNT(*) AS row_count,
        SUM(CASE WHEN holder_region = 'East' THEN holder_risk_score_share_of_holder_id ELSE 0 END) AS holder_risk_score_share_of_holder_id_holder_regi,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM top_per_group
    WHERE holder_risk_score_vs_holder_id_avg < 0.4
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT
    end_date_ym,
    row_count,
    holder_risk_score_share_of_holder_id_holder_regi,
    product_list
FROM stats

WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m
    FROM policies
    WHERE end_date < DATE '2026-07-11'
      AND start_date BETWEEN DATE '2024-11-20' AND DATE '2025-07-07'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.start_date_plus_12m,
        p.risk_score AS holder_risk_score
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(start_date_plus_12m)) AS rows_in_start_date_plus_12m_year,
        FIRST_VALUE(start_date_plus_12m) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_plus_12m,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY holder_risk_score DESC, id) AS holder_risk_score_dense_rank,
        LEAD(end_date, 2) OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score, id) AS next2_end_date,
        LEAD(end_date) OVER (ORDER BY start_date_plus_12m, id) AS next_end_date,
        MAX(holder_risk_score) OVER (PARTITION BY YEAR(start_date_plus_12m)) AS start_date_plus_12m_year_max_holder_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score), 3) AS holder_risk_score_percent_rank,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        MAX(holder_risk_score) OVER (PARTITION BY product ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_holder_risk_score_so_far,
        ROUND(PERCENT_RANK() OVER w_end_date_year_by_start_date, 3) AS start_date_plus_12m_percent_rank,
        RANK() OVER w_status_holder_id_by_holder_r_3 AS holder_risk_score_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id, product)) AS distinct_status_in_holder_id_product,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY YEAR(start_date_plus_12m)), 2) AS holder_risk_score_vs_start_date_plus_12m_year_av,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_4,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_id, product), 2) AS holder_id_product_avg_holder_risk_score
    FROM joined_holder
    WINDOW w_start_date_plus_12m_year AS (PARTITION BY YEAR(start_date_plus_12m)), w_end_date_year_by_start_date AS (PARTITION BY YEAR(end_date) ORDER BY start_date_plus_12m, id), w_status_holder_id_by_holder_r_3 AS (PARTITION BY status, holder_id ORDER BY holder_risk_score)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(holder_risk_score_avg_4 / NULLIF(holder_risk_score, 0), 3) AS holder_risk_score_avg_4_per_holder_risk_score,
        ABS(holder_risk_score_avg_4 - 0.5) AS holder_risk_score_avg_4_distance,
        GREATEST(holder_risk_score_avg_4, holder_risk_score_rank) AS greatest_holder_risk_score_avg_4_holder_risk_sco
    FROM with_trends
    WHERE start_date_plus_12m_percent_rank BETWEEN 0.1 AND 0.6
)
SELECT
    id,
    end_date,
    status,
    start_date_plus_12m,
    holder_risk_score,
    first_known_start_date_plus_12m,
    next2_end_date,
    start_date_plus_12m_year_max_holder_risk_score,
    holder_risk_score_percent_rank,
    second_end_date,
    max_holder_risk_score_so_far,
    start_date_plus_12m_percent_rank,
    holder_risk_score_rank,
    distinct_status_in_holder_id_product,
    holder_risk_score_vs_start_date_plus_12m_year_av,
    holder_risk_score_avg_4,
    holder_id_product_avg_holder_risk_score,
    holder_risk_score_avg_4_distance,
    greatest_holder_risk_score_avg_4_holder_risk_sco,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
    ABS(holder_risk_score_avg_4 - 0.5) AS holder_risk_score_avg_4_distance_2
FROM labeled

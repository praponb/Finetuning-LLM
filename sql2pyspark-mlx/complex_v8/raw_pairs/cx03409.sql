WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE end_date IS NOT NULL
      AND product = 'TRAVEL'
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.start_date_month_start,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score DESC NULLS FIRST) AS holder_risk_score_dense_rank,
        MIN(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), MONTH(start_date_month_start)) AS holder_joined_on_month_start_date_month_start_mo,
        ROW_NUMBER() OVER w_holder_joined_on_year_holder AS seq_no,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), MONTH(start_date_month_start)) AS rows_in_holder_joined_on_month_start_date_month_,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_segment), 0), 4) AS holder_risk_score_share_of_holder_segment,
        COUNT(*) OVER (PARTITION BY holder_segment) AS rows_in_holder_segment,
        DENSE_RANK() OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score DESC NULLS FIRST, id) AS holder_risk_score_dense_rank_2,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_3,
        holder_risk_score - LAG(holder_risk_score) OVER (PARTITION BY YEAR(holder_joined_on), holder_segment ORDER BY start_date_month_start DESC, id) AS holder_risk_score_change,
        LAG(holder_risk_score, 2) OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score DESC NULLS FIRST, id) AS prev2_holder_risk_score
    FROM with_parent
    WINDOW w_holder_joined_on_year_holder AS (PARTITION BY YEAR(holder_joined_on), holder_segment ORDER BY start_date_month_start DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        start_date_month_start,
        holder_risk_score,
        holder_segment,
        holder_risk_score_dense_rank,
        holder_joined_on_month_start_date_month_start_mo,
        seq_no,
        rows_in_holder_joined_on_month_start_date_month_,
        holder_risk_score_share_of_holder_segment,
        rows_in_holder_segment,
        holder_risk_score_dense_rank_2,
        holder_risk_score_avg_3,
        holder_risk_score_change,
        prev2_holder_risk_score,
        ROUND(prev2_holder_risk_score * 1.1, 2) AS prev2_holder_risk_score_adj
    FROM with_trends
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        start_date,
        holder_segment,
        holder_risk_score_dense_rank,
        holder_joined_on_month_start_date_month_start_mo,
        seq_no,
        holder_risk_score_share_of_holder_segment,
        rows_in_holder_segment,
        holder_risk_score_dense_rank_2,
        holder_risk_score_change,
        prev2_holder_risk_score,
        COUNT(*) OVER (PARTITION BY status, holder_id) AS rows_in_status_holder_id,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product, MONTH(start_date_month_start) ORDER BY rows_in_holder_joined_on_month_start_date_month_, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status, holder_id)) AS distinct_product_in_status_holder_id,
        LAG(rows_in_holder_segment, 3) OVER w_product_start_date_month_sta_2 AS prev3_rows_in_holder_segment,
        MAX(holder_risk_score) OVER (PARTITION BY status, holder_id) AS status_holder_id_max_holder_risk_score,
        SUM(holder_risk_score_change) OVER w_status_holder_id AS status_holder_id_sum_holder_risk_score_change
    FROM labeled
    WINDOW w_product_start_date_month_sta_2 AS (PARTITION BY product, MONTH(start_date_month_start) ORDER BY rows_in_holder_joined_on_month_start_date_month_, id), w_status_holder_id AS (PARTITION BY status, holder_id)
)
SELECT
    id,
    start_date,
    holder_segment,
    holder_risk_score_dense_rank,
    holder_joined_on_month_start_date_month_start_mo,
    seq_no,
    holder_risk_score_share_of_holder_segment,
    rows_in_holder_segment,
    prev2_holder_risk_score,
    rows_in_status_holder_id,
    second_start_date,
    distinct_product_in_status_holder_id,
    prev3_rows_in_holder_segment,
    status_holder_id_max_holder_risk_score,
    DATEDIFF(second_start_date, start_date) AS days_start_date_to_second_start_date
FROM with_windows
ORDER BY prev2_holder_risk_score, id
LIMIT 10

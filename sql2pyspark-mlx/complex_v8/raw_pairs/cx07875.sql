WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE premium BETWEEN 918.1 AND 4688.7
      AND YEAR(start_date) = 2025
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        p.segment AS holder_segment
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score < 0.6
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_holder_id_by_start_date AS start_date_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id), 3) AS start_date_percent_rank,
        DENSE_RANK() OVER w_holder_id_by_start_date_ties AS start_date_dense_rank,
        LAG(start_date) OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC, id) AS prev_start_date,
        LEAD(start_date) OVER w_start_date_year_by_start_dat AS next_start_date,
        ROUND(PERCENT_RANK() OVER w_holder_id_start_date_year_by, 3) AS start_date_percent_rank_2,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        LEAD(start_date, 3) OVER w_holder_id_start_date_year_by AS next3_start_date,
        LAG(start_date) OVER (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id) AS prev_start_date_2,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY start_date, id) AS start_date_dense_rank_2,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM with_details
    WINDOW w_holder_id_by_start_date AS (PARTITION BY holder_id ORDER BY start_date, id), w_holder_id_by_start_date_ties AS (PARTITION BY holder_id ORDER BY start_date), w_start_date_year_by_start_dat AS (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id), w_holder_id_start_date_year_by AS (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE start_date_dense_rank_2 = 1
      AND start_date_percent_rank <= 0.6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        holder_segment,
        start_date_percent_rank,
        prev_start_date,
        next_start_date,
        first_known_start_date,
        next3_start_date,
        prev_start_date_2,
        start_date_dense_rank_2,
        second_start_date,
        ROUND(AVG(start_date_dense_rank) OVER (PARTITION BY start_date_dense_rank_2), 2) AS start_date_dense_rank_2_avg_start_date_dense_ran,
        ROUND(AVG(start_date_dense_rank) OVER w_prev_start_date_month, 2) AS prev_start_date_month_avg_start_date_dense_rank,
        LEAD(second_start_date) OVER (ORDER BY next3_start_date, id) AS next_second_start_date
    FROM first_rows
    WINDOW w_prev_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', prev_start_date))
)
-- Final projection
SELECT
    id,
    holder_id,
    holder_segment,
    start_date_percent_rank,
    prev_start_date,
    first_known_start_date,
    next3_start_date,
    prev_start_date_2,
    start_date_dense_rank_2,
    second_start_date,
    start_date_dense_rank_2_avg_start_date_dense_ran,
    prev_start_date_month_avg_start_date_dense_rank
FROM metrics
ORDER BY holder_segment, holder_id

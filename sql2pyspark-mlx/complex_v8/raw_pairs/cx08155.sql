WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE end_date >= DATE '2025-11-20'
      AND start_date >= DATE '2024-11-20'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id) AS start_date_rank,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id)) AS days_since_prev_start_date,
        LEAD(start_date) OVER w_start_date_year_product_by_s AS next_start_date,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        RANK() OVER (PARTITION BY product, holder_id ORDER BY start_date DESC) AS start_date_rank_2,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY start_date, id)) AS days_since_prev_end_date,
        LAG(start_date) OVER w_all_by_end_date AS prev_start_date,
        FIRST_VALUE(start_date) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY product, holder_id ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        DENSE_RANK() OVER w_holder_id_start_date_month_b_2 AS start_date_dense_rank,
        LAG(start_date) OVER w_holder_id_start_date_month_b_2 AS prev_start_date_2,
        RANK() OVER w_start_date_year_product_by_s AS start_date_rank_3,
        LAST_VALUE(start_date) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date
    FROM filtered
    WINDOW w_start_date_year_product_by_s AS (PARTITION BY YEAR(start_date), product ORDER BY start_date, id), w_all_by_end_date AS (ORDER BY end_date, id), w_holder_id_start_date_month_b_2 AS (PARTITION BY holder_id, MONTH(start_date) ORDER BY start_date, id)
),
enriched AS (
    -- Label and score rows
    SELECT *, COALESCE(start_date_rank, 0) AS start_date_rank_filled
    FROM with_trends
)
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    last_known_start_date,
    days_since_prev_start_date,
    next_start_date,
    second_start_date,
    first_known_start_date,
    start_date_rank_2,
    days_since_prev_end_date,
    start_date_cume_dist,
    start_date_dense_rank,
    prev_start_date_2,
    start_date_rank_3,
    last_start_date,
    start_date_rank_filled
FROM enriched
ORDER BY start_date_dense_rank DESC, start_date_rank_3

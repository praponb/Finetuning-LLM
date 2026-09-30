WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', product, status) AS product_status_label,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM policies
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.end_date_label,
        p.segment AS holder_segment
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        start_date,
        RANK() OVER w_holder_segment_end_date_mont AS start_date_rank,
        DENSE_RANK() OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id) AS start_date_dense_rank,
        LAG(end_date) OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date, id) AS prev_end_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_start_date_month_holder_id_b) AS days_since_prev_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(CUME_DIST() OVER w_product_holder_id_by_start_d_3, 3) AS start_date_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_segment ORDER BY end_date), 3) AS end_date_cume_dist,
        LEAD(start_date, 2) OVER (PARTITION BY product ORDER BY end_date ASC, id) AS next2_start_date,
        LEAD(start_date, 3) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date NULLS LAST, id) AS next3_start_date,
        LAG(start_date, 2) OVER (PARTITION BY holder_segment, DATE_TRUNC('MONTH', end_date) ORDER BY start_date, id) AS prev2_start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_segment ORDER BY end_date, id), 3) AS end_date_cume_dist_2,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        ROUND(PERCENT_RANK() OVER w_holder_id_end_date_month_by, 3) AS start_date_percent_rank,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM combined_details
    WINDOW w_holder_segment_end_date_mont AS (PARTITION BY holder_segment, DATE_TRUNC('MONTH', end_date) ORDER BY start_date), w_start_date_month_holder_id_b AS (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date, id), w_product_holder_id_by_start_d_3 AS (PARTITION BY product, holder_id ORDER BY start_date DESC), w_holder_id_end_date_month_by AS (PARTITION BY holder_id, MONTH(end_date) ORDER BY start_date NULLS LAST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        product,
        start_date,
        start_date_dense_rank,
        prev_end_date,
        days_since_prev_start_date,
        first_start_date,
        start_date_cume_dist,
        end_date_cume_dist,
        next2_start_date,
        next3_start_date,
        prev2_start_date,
        end_date_cume_dist_2,
        first_known_start_date,
        start_date_percent_rank,
        last_known_start_date
    FROM windowed
    WHERE start_date_rank <= 5
      AND start_date_dense_rank <= 7
)
-- Final projection
SELECT
    id,
    product,
    start_date,
    start_date_dense_rank,
    prev_end_date,
    first_start_date,
    start_date_cume_dist,
    next3_start_date,
    prev2_start_date,
    first_known_start_date,
    last_known_start_date,
    LOWER(TRIM(product)) AS product_clean
FROM first_rows
ORDER BY last_known_start_date NULLS LAST, start_date_dense_rank DESC, id
LIMIT 10

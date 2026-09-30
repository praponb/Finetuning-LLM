WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
      AND start_date >= DATE '2024-08-27'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        p.segment AS holder_segment
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        start_date,
        end_date,
        status,
        holder_segment,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product, holder_segment ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        LAST_VALUE(end_date) OVER (PARTITION BY holder_segment, status ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        FIRST_VALUE(end_date) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date, id) AS rn,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_segment ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        LAG(end_date) OVER (ORDER BY start_date ASC, id) AS prev_end_date,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        LEAD(start_date) OVER w_all_by_start_date AS next_start_date,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product, holder_segment ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY product, holder_segment ORDER BY end_date ASC, id)) AS days_since_prev_end_date,
        RANK() OVER w_all_by_start_date AS start_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY end_date), 3) AS end_date_percent_rank,
        ROW_NUMBER() OVER (ORDER BY start_date ASC, id) AS rn_2,
        FIRST_VALUE(end_date) OVER (PARTITION BY product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date_2
    FROM with_details
    WINDOW w_all_by_start_date AS (ORDER BY start_date ASC, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        status,
        holder_segment,
        first_known_end_date,
        last_end_date,
        rn,
        first_start_date,
        prev_end_date,
        last_known_end_date,
        next_start_date,
        end_date_percent_rank,
        FIRST_VALUE(end_date_percent_rank) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', second_start_date) ORDER BY last_known_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_segment, MONTH(prev_end_date) ORDER BY second_start_date ASC), 3) AS second_start_date_percent_rank,
        MAX(days_since_prev_end_date) OVER (PARTITION BY rn_2 ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_prev_end_date_so_far,
        ROUND(AVG(rn_2) OVER w_prev_end_date_year, 2) AS prev_end_date_year_avg_rn_2,
        RANK() OVER (PARTITION BY holder_segment, MONTH(prev_end_date) ORDER BY second_start_date ASC) AS second_start_date_rank
    FROM sequenced
    WINDOW w_prev_end_date_year AS (PARTITION BY YEAR(prev_end_date))
)
-- Final projection
SELECT
    id,
    status,
    holder_segment,
    first_known_end_date,
    prev_end_date,
    last_known_end_date,
    next_start_date,
    end_date_percent_rank,
    first_known_end_date_percent_rank,
    second_start_date_percent_rank,
    max_days_since_prev_end_date_so_far,
    prev_end_date_year_avg_rn_2,
    second_start_date_rank,
    CONCAT(holder_segment, '#', CAST(rn AS STRING)) AS holder_segment_ref,
    CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
FROM sequenced_2
ORDER BY second_start_date_percent_rank NULLS LAST, max_days_since_prev_end_date_so_far, id
LIMIT 10

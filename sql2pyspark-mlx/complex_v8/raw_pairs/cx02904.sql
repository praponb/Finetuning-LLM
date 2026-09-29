WITH prepared AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY MONTH(start_date)) AS start_date_month_max_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY MONTH(start_date)), 0), 4) AS premium_share_of_start_date_month,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        SUM(premium) OVER w_start_date_month AS start_date_month_sum_premium,
        NTILE(5) OVER (ORDER BY start_date DESC, id) AS start_date_quintile,
        LAST_VALUE(premium) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        DENSE_RANK() OVER (ORDER BY end_date DESC, id) AS end_date_dense_rank,
        DENSE_RANK() OVER (ORDER BY start_date DESC, id) AS start_date_dense_rank,
        ROW_NUMBER() OVER (ORDER BY premium NULLS LAST, id) AS premium_seq
    FROM prepared
    WINDOW w_start_date_month AS (PARTITION BY MONTH(start_date))
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE start_date_dense_rank = 1
)
SELECT
    id,
    holder_id,
    start_date,
    status,
    start_date_month_max_premium,
    premium_share_of_start_date_month,
    premium_avg_1_prior,
    start_date_month_sum_premium,
    start_date_quintile,
    last_premium,
    end_date_dense_rank,
    start_date_dense_rank,
    premium_seq,
    CASE WHEN product IN ('LIFE', 'AUTO') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
FROM first_rows
WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-03-23'

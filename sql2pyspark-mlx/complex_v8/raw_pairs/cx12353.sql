WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        MONTH(start_date) AS start_date_month
    FROM policies
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_group,
        ROUND(AVG(premium) OVER (ORDER BY start_date_month DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        RANK() OVER (ORDER BY start_date_month DESC) AS start_date_month_rank
    FROM prepared_policys
)
-- Subtotals with GROUPING SETS
SELECT
    start_date_month_rank,
    status,
    status_group,
    SUM(start_date_month_rank) AS total_start_date_month_rank
FROM sequenced
GROUP BY GROUPING SETS ((start_date_month_rank, status, status_group), (start_date_month_rank), (status), ())

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
    WHERE status NOT IN ('EXPIRED', 'CANCELLED')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium, id)) AS days_since_prev_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7
    FROM prepared_policys
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, premium_avg_3, premium_avg_7
    FROM with_windows
) src
UNPIVOT (
    amount FOR measure IN (premium_avg_3, premium_avg_7)
) unp

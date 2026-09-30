WITH src AS (
    SELECT *
    FROM orders
    WHERE amount BETWEEN 720 AND 1550 AND order_date > '2023-10-01' AND amount <= 1520
),
OrdersStats AS (
    SELECT channel, SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count
    FROM src
    GROUP BY channel
)
SELECT channel, mobile_count
FROM OrdersStats;

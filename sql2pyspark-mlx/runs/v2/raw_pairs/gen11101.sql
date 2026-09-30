WITH ranked AS (
    SELECT
        id,
        status,
        end_date,
        start_date,
        RANK() OVER (PARTITION BY status ORDER BY end_date DESC) row_num
    FROM subscriptions
    WHERE end_date <= '2024-08-01'
)
SELECT
    id,
    status,
    end_date,
    start_date
FROM ranked
WHERE row_num <= 2
ORDER BY id;

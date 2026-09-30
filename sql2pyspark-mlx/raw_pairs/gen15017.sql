WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE start_date >= DATE '2023-07-23'
      AND customer_id IS NOT NULL
)
SELECT customer_id, id
FROM cleaned
EXCEPT
SELECT customer_id, id
FROM subscriptions
WHERE start_date < '2023-05-01'

WITH base AS (
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE end_date >= DATE '2023-07-21'
      AND customer_id IS NULL
)
SELECT
    c.city,
    c.country,
    COUNT(*) AS cnt,
    ROUND(SUM(e.monthly_fee_filled), 2) AS total_monthly_fee_filled,
    GROUPING_ID() AS grouping_level
FROM base e
INNER JOIN customers c ON e.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, c.country), (c.city), ())
ORDER BY total_monthly_fee_filled DESC;

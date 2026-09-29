WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(status, 1, 4) AS status_prefix,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2023-05-31'
      AND customer_id IS NOT NULL
)
-- Non-equi join on a validity window
SELECT
    src.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM valid src
INNER JOIN subscriptions sub
    ON src.customer_id = sub.customer_id
   AND src.order_date >= sub.start_date
   AND src.order_date < sub.end_date + INTERVAL '7' DAY

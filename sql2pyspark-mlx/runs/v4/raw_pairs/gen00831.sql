WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(status) AS status_upper,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
)
-- Non-equi join on a validity window
SELECT
    e.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM valid e
LEFT JOIN subscriptions sub
    ON e.customer_id = sub.customer_id
   AND e.order_date BETWEEN sub.start_date AND sub.end_date;

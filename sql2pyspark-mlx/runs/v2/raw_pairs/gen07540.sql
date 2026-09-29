SELECT
    channel,
    order_quarter,
    mobile_count,
    amount_count
FROM (
    SELECT
        channel,
        QUARTER(order_date) AS order_quarter,
        SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count,
        COUNT(amount) AS amount_count
    FROM orders
    WHERE status = 'COMPLETED'
    GROUP BY channel, QUARTER(order_date)
) AS t
WHERE mobile_count < 57
ORDER BY order_quarter DESC;

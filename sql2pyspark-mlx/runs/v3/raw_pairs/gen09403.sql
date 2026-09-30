SELECT
    order_month,
    channel,
    shipping_fee_range,
    pct_shipped
FROM (
    SELECT
        DATE_TRUNC('MONTH', order_date) AS order_month,
        channel,
        MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range,
        ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
    FROM orders
    WHERE amount > 1530
    GROUP BY DATE_TRUNC('MONTH', order_date), channel
) AS agg
WHERE shipping_fee_range >= 33

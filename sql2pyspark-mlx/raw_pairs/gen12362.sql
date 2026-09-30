SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    status,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(DISTINCT channel) AS distinct_channels,
    SUM(amount) AS total_amount,
    COUNT_IF(channel = 'web') AS web_events,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', order_date) ORDER BY SIZE(COLLECT_SET(channel)) DESC) AS num_channels_dense_rank
FROM orders
GROUP BY DATE_TRUNC('QUARTER', order_date), status
HAVING web_events < 1000;

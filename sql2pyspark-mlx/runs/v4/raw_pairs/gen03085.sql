SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN stock_qty >= 424 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
    COUNT_IF(brand = 'Globex') AS globex_events,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    PERCENTILE_APPROX(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date) ORDER BY COUNT_IF(category = 'Fashion') DESC) AS fashion_events_rank
FROM products
GROUP BY DATE_TRUNC('WEEK', launch_date), CASE WHEN stock_qty >= 424 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY stock_qty_bit_or

SELECT
    country,
    segment,
    ROUND((100 * SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sg,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values
FROM customers
GROUP BY ROLLUP(country, segment)

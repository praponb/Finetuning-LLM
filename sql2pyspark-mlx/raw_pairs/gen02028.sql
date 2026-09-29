SELECT
    order_id,
    product_id,
    CASE WHEN unit_price >= 350 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM order_items
GROUP BY order_id, product_id, CASE WHEN unit_price >= 350 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY unit_price_tier NULLS FIRST;

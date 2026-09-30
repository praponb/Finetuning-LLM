SELECT
    CASE WHEN quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    PERCENTILE_APPROX(unit_price, 0.95) AS unit_price_p95,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING quantity_stddev >= 0
ORDER BY quantity_tier DESC;

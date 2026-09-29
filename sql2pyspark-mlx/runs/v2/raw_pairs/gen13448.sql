SELECT
    CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(unit_price) AS total_unit_price,
    COUNT(*) AS cnt,
    ROUND(SUM(unit_price) * 100.0 / SUM(SUM(unit_price)) OVER (), 2) AS pct_of_total
FROM order_items
WHERE unit_price IS NOT NULL
GROUP BY CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END
ORDER BY cnt ASC

SELECT
    id,
    store_id,
    sale_date,
    units,
    CASE WHEN units >= 39 THEN 'High' WHEN units >= 11 THEN 'Medium' ELSE 'Low' END AS units_band,
    units + revenue AS revenue_per_unit
FROM store_sales
WHERE units IS NOT NULL
ORDER BY sale_date, units DESC

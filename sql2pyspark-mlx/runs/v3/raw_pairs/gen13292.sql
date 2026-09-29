SELECT *, GREATEST(units, revenue) AS greatest_units_revenue, ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales
WHERE revenue BETWEEN 3200 AND 3800 AND revenue >= 3900 AND units < 34
ORDER BY product_id DESC;

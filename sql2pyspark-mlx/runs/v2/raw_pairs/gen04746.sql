SELECT *, COALESCE(units, 0) AS units_filled, ROUND(units * 1.07, 2) AS units_adjusted
FROM store_sales
ORDER BY 5, id DESC

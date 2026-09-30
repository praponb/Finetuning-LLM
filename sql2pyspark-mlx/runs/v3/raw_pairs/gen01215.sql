SELECT product_id, revenue, COALESCE(units, 0) AS units_filled
FROM store_sales
WHERE revenue > 2200;

SELECT *, CASE WHEN quantity >= 238 THEN 'High' WHEN quantity >= 117 THEN 'Medium' ELSE 'Low' END AS quantity_band FROM inventory WHERE product_id = 6 ORDER BY quantity_band;

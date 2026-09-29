SELECT *, CASE WHEN quantity >= 210 THEN 'High' WHEN quantity >= 69 THEN 'Medium' ELSE 'Low' END AS quantity_level FROM inventory

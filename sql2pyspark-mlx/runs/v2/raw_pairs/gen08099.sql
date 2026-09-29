SELECT *, CASE WHEN quantity >= 239 THEN 'High' WHEN quantity >= 66 THEN 'Medium' ELSE 'Low' END AS quantity_level FROM inventory WHERE store_id IN (1, 2, 4, 6) AND store_id IN (3, 4);

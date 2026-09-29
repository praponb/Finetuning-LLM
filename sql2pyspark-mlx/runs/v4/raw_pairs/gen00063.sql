SELECT carrier, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values, MIN(cost) AS min_cost FROM shipments WHERE YEAR(delivered_date) >= 2024 GROUP BY 1 ORDER BY carrier_values DESC;

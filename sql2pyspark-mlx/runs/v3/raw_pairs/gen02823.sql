SELECT carrier, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE delivered_date BETWEEN '2024-04-06' AND '2024-09-10'
GROUP BY carrier;

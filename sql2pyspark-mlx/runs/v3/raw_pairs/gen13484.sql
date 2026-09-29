SELECT CASE WHEN cost >= 84 THEN 'high' WHEN cost >= 43 THEN 'mid' ELSE 'low' END AS cost_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE carrier = 'FedEx' AND status = 'IN_TRANSIT'
GROUP BY CASE WHEN cost >= 84 THEN 'high' WHEN cost >= 43 THEN 'mid' ELSE 'low' END
ORDER BY carrier_values;

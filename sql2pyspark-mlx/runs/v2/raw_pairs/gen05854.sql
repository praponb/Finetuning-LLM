SELECT carrier, status, COUNT(DISTINCT cost) AS unique_statuss
FROM shipments
WHERE carrier != 'Flash'
GROUP BY carrier, status;

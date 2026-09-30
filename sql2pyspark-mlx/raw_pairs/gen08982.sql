SELECT *
FROM (
    SELECT carrier, COUNT(DISTINCT cost) AS unique_carriers
    FROM shipments
    GROUP BY carrier
) AS s
WHERE unique_carriers >= 7
ORDER BY carrier DESC
LIMIT 5

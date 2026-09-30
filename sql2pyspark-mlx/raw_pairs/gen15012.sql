SELECT COUNT(CASE WHEN carrier = 'Flash' THEN 1 END) AS flash_count
FROM shipments
ORDER BY flash_count;

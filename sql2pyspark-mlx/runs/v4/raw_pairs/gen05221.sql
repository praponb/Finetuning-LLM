SELECT ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_ups, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM shipments
WHERE carrier = 'UPS' AND carrier = 'UPS'
ORDER BY pct_ups;

SELECT *, UPPER(status) AS status_upper
FROM shipments
ORDER BY delivered_date DESC, id
LIMIT 20;

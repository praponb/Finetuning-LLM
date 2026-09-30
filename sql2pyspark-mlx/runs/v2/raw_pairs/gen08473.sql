SELECT id, shipped_date, ROUND(cost, 2) AS cost_rounded
FROM shipments
WHERE delivered_date >= '2023-07-01'
ORDER BY id DESC, cost_rounded DESC
LIMIT 3;

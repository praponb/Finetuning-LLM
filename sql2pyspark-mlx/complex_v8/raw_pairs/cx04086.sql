SELECT capacity AS ref_id, opened_on AS day, id AS ref_id_2
FROM warehouses
UNION ALL
SELECT id, opened_on, capacity
FROM warehouses
ORDER BY day, ref_id

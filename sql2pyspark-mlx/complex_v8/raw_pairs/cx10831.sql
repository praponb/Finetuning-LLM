SELECT capacity AS entity_id, opened_on AS day
FROM warehouses
WHERE capacity BETWEEN 4912 AND 11506
UNION ALL
SELECT capacity, opened_on
FROM warehouses
WHERE capacity BETWEEN 2614 AND 11506

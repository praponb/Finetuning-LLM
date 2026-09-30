SELECT city AS name, capacity AS key_id, opened_on AS event_date
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
  AND YEAR(opened_on) = 2025
UNION
SELECT wh_code AS name, id AS key_id, opened_on AS event_date
FROM warehouses

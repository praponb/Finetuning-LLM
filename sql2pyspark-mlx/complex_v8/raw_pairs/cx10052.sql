SELECT capacity AS key_id, opened_on AS event_date
FROM warehouses
WHERE opened_on >= DATE '2024-11-25'
  AND city <> 'Kuala Lumpur'
UNION ALL
SELECT capacity, opened_on
FROM warehouses
ORDER BY key_id, event_date

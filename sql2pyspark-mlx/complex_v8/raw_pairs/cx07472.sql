SELECT capacity AS ref_id, opened_on
FROM warehouses
WHERE capacity <= 11506
UNION ALL
SELECT id, opened_on
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
  AND city <> 'Bangkok'

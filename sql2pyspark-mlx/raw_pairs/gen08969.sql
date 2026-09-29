SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC NULLS LAST, id) AS rn
FROM shipments
QUALIFY rn = 1;

SELECT
    id,
    carrier,
    shipped_date,
    ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS row_num
FROM shipments;

SELECT
    id,
    status,
    delivered_date,
    shipped_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS rnk
    FROM shipments
) AS ranked
WHERE rnk <= 3
ORDER BY status DESC

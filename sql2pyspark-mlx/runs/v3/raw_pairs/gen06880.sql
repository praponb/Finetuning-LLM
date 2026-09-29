SELECT
    id,
    status,
    cost,
    order_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS rnk
    FROM shipments
) AS t
WHERE rnk <= 1
ORDER BY 3 DESC;

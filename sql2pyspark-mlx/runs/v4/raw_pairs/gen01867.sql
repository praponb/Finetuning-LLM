SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS rnk
    FROM shipments
) AS t
WHERE rnk = 1;

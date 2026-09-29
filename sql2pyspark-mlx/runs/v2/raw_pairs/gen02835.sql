SELECT
    id,
    order_id,
    delivered_date,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS rnk
    FROM shipments
    WHERE cost BETWEEN 35 AND 40
) AS ranked
WHERE rnk <= 3;

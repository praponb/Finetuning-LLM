SELECT *
FROM (
    SELECT
        id,
        order_id,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS row_num
    FROM shipments
) AS t
WHERE row_num <= 2;

WITH ranked_shipments AS (
    SELECT
        id,
        order_id,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) row_num
    FROM shipments
    WHERE status = 'IN_TRANSIT'
)
SELECT *
FROM ranked_shipments
WHERE row_num = 1;

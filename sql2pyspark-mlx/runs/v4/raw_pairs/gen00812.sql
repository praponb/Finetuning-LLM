WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) rn
    FROM shipments
)
SELECT *
FROM ranked
WHERE rn <= 2
ORDER BY id DESC;

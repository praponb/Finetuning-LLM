WITH kept_keys AS (
    SELECT order_id, id
    FROM shipments
    EXCEPT ALL
    SELECT order_id, id
    FROM shipments
    WHERE cost BETWEEN 27 AND 69
),
verified AS (
    SELECT s.*
    FROM shipments s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    carrier,
    shipped_date,
    delivered_date,
    cost
FROM verified;

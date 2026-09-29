WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM shipments
    EXCEPT DISTINCT
    SELECT status, id
    FROM shipments
    WHERE cost BETWEEN 79 AND 81
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    CASE WHEN carrier RLIKE '^(Flash|FedEx)$' THEN 'CORE' ELSE 'EXTENDED' END AS carrier_zone
FROM reconciled
ORDER BY delivered_date, id DESC;

WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM suppliers
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE city = 'Singapore'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    CASE WHEN supplier_name RLIKE '^(Pacific Wholesale|Tokyo Traders)$' THEN 'EXTENDED' WHEN supplier_name RLIKE '^(Siam Parts Co|Global Supply)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS supplier_name_zone
FROM reconciled;

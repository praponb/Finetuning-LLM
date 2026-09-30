WITH kept_keys AS (
    SELECT id
    FROM suppliers
    EXCEPT ALL
    SELECT id
    FROM suppliers
    WHERE city IN ('Bangkok', 'New York')
),
reconciled AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    supplier_name,
    country
FROM reconciled
ORDER BY country, id;

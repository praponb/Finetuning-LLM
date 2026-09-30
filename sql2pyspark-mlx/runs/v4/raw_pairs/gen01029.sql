WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS FIRST) AS dedup_rank
        FROM products
    ) t
    WHERE dedup_rank = 1
),
ranked_products AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY cost DESC NULLS FIRST) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM ranked_products
    EXCEPT
    SELECT supplier_id
    FROM products
    WHERE category IN ('Fashion', 'Electronics')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_products s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND YEAR(ch.last_restock) = 2023
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        brand,
        cost,
        price
    FROM not_flagged
) src
UNPIVOT (
    value FOR kpi IN (cost AS COST, price AS PRICE_VAL)
)

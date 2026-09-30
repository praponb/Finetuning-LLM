WITH matched_keys AS (
    SELECT id, store_id
    FROM store_sales
    EXCEPT ALL
    SELECT id, store_id
    FROM store_sales
    WHERE units > 30
),
reconciled AS (
    SELECT s.*
    FROM store_sales s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units
FROM reconciled
ORDER BY units DESC NULLS LAST

WITH kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM store_sales
    EXCEPT
    SELECT product_id, id
    FROM store_sales
    WHERE YEAR(sale_date) >= 2025
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue
FROM reconciled
ORDER BY store_id DESC;

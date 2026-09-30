WITH RankedStoreSales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) rnk
    FROM store_sales
)
SELECT
    id,
    product_id,
    units,
    sale_date
FROM RankedStoreSales
WHERE rnk <= 3;

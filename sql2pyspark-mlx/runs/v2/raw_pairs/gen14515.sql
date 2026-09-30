WITH RankedStoreSales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS rnk
    FROM store_sales
)
SELECT
    id,
    store_id,
    sale_date,
    units
FROM RankedStoreSales
WHERE rnk <= 5
ORDER BY units, sale_date

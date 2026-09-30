WITH RankedStoreSales AS (
    SELECT
        id,
        store_id,
        units,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS pos
    FROM store_sales
)
SELECT
    id,
    store_id,
    units,
    sale_date
FROM RankedStoreSales
WHERE pos <= 3
ORDER BY sale_date DESC

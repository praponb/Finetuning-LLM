SELECT id, store_id, units
FROM (
    SELECT
        id,
        store_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS row_num
    FROM store_sales
) AS r
WHERE row_num <= 3
ORDER BY units DESC;

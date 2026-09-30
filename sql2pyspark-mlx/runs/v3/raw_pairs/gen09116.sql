WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS pos
    FROM store_sales
)
SELECT *
FROM numbered
WHERE pos <= 3
ORDER BY id DESC

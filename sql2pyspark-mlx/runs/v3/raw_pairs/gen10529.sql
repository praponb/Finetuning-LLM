SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS pos
    FROM store_sales
) AS t
WHERE pos <= 3

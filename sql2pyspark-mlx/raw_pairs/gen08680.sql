SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS rnk
    FROM store_sales
    WHERE revenue BETWEEN 2200 AND 2800
) AS ranked
WHERE rnk <= 1
ORDER BY units DESC;

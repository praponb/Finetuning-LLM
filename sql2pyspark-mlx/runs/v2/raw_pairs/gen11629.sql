SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) row_num
    FROM stores
) ranked
WHERE row_num <= 5
ORDER BY store_name

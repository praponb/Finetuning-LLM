SELECT id, store_id, revenue
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS rn
    FROM store_sales
) AS ranked
WHERE rn <= 3
ORDER BY id DESC, store_id DESC

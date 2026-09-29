SELECT id, revenue
FROM store_sales
WHERE units IS NULL AND store_id IN (1, 4, 5, 6)
ORDER BY revenue, id DESC;

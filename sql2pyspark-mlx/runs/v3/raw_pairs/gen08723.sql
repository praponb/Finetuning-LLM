SELECT
    s.id,
    s.opened_date,
    s.store_name,
    s.region
FROM stores s
WHERE s.opened_date BETWEEN '2023-08-01' AND '2024-02-01' AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
ORDER BY s.opened_date

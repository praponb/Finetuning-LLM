WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) pos
    FROM stores
)
SELECT id, region, opened_date
FROM numbered
WHERE pos = 1
ORDER BY id DESC

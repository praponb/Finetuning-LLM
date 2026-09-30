SELECT id, region, opened_date
FROM (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date) AS pos
    FROM stores
) AS t
WHERE pos <= 3
ORDER BY 1 DESC, region

WITH filtered AS (
    -- Scope the warehouses rows
    SELECT *
    FROM warehouses
    WHERE LENGTH(TRIM(wh_code)) >= 7
)
SELECT city
FROM filtered
WHERE capacity >= 4912
EXCEPT ALL
SELECT category
FROM skus
ORDER BY city DESC

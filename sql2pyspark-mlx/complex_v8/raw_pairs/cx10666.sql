WITH valid AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
      AND city <> 'Kuala Lumpur'
)
SELECT capacity, id AS ref_id
FROM valid
UNION
SELECT capacity, id
FROM warehouses
WHERE LOWER(wh_code) LIKE '%bk%'

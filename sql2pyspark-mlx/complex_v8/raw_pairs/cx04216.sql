WITH unioned AS (
    -- Stack the branches
    SELECT id AS ref_id, city, capacity
    FROM warehouses
    UNION
    SELECT capacity, wh_code, id
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
      AND capacity >= 2614
)
SELECT city, COUNT(*) AS n_rows, SUM(capacity) AS total_capacity
FROM unioned
GROUP BY city
ORDER BY city

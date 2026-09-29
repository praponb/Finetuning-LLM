WITH unioned AS (
    -- Stack the branches
    SELECT wh_code AS label, city AS label_2, opened_on
    FROM warehouses
    UNION ALL
    SELECT city, wh_code, opened_on
    FROM warehouses
)
SELECT label, COUNT(*) AS n_rows, MAX(opened_on) AS latest_opened_on
FROM unioned
GROUP BY label
ORDER BY label

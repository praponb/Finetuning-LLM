SELECT id, city, opened_date
FROM (
    SELECT
        id,
        city,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS pos
    FROM stores
    WHERE region IN ('West', 'Central', 'South')
) AS t
WHERE pos = 1
ORDER BY opened_date;

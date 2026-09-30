SELECT
    id,
    city,
    opened_date,
    region
FROM (
    SELECT
        id,
        city,
        opened_date,
        region,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) pos
    FROM stores
    WHERE opened_date > '2023-11-07'
) r
WHERE pos <= 3;

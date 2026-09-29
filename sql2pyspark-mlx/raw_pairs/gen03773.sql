SELECT id, city, sqft
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
    FROM stores
    WHERE region <> 'North'
) AS t
WHERE rn = 1
ORDER BY id DESC;

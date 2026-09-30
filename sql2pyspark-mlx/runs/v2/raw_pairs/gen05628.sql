SELECT
    id,
    segment,
    credit_limit,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS pos
    FROM customers
) AS r
WHERE pos <= 3
ORDER BY segment;

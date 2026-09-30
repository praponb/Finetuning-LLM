SELECT id, city, credit_limit
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) row_num
    FROM customers
) r
WHERE row_num <= 5
ORDER BY id DESC;

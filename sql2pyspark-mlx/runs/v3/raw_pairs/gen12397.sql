SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit, id) AS rnk
    FROM customers
) AS ranked
WHERE rnk = 1
ORDER BY signup_date;

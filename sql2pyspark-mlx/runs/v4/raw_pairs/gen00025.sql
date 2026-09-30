SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) row_num
    FROM accounts
) r
WHERE row_num <= 3

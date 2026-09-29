SELECT
    id,
    account_type,
    opened_date,
    DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS opened_date_dense_rank
FROM accounts
WHERE opened_date BETWEEN '2023-05-23' AND '2024-03-01'
ORDER BY opened_date_dense_rank DESC, id;

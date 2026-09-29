WITH Base AS (
    SELECT *
    FROM customers
    WHERE signup_date < '2024-02-09' OR age > 32
),
CustomersStats AS (
    SELECT segment, MAX(credit_limit) AS max_credit_limit
    FROM Base
    GROUP BY segment
)
SELECT *
FROM CustomersStats
WHERE max_credit_limit > 6100;

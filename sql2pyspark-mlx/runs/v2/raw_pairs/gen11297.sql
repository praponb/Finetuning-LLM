WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) AS rn
    FROM accounts
)
SELECT *
FROM numbered
WHERE rn <= 2
ORDER BY id

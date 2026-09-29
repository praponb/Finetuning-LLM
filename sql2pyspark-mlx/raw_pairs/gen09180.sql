WITH top_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS pos
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE pos = 1;

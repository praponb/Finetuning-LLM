WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS pos
    FROM transactions
    WHERE merchant IN ('Shell', 'Grab', 'Lazada')
)
SELECT *
FROM ranked
WHERE pos <= 2
ORDER BY merchant

WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) pos
    FROM subscriptions
)
SELECT *
FROM Numbered
WHERE pos <= 3
ORDER BY pos DESC;

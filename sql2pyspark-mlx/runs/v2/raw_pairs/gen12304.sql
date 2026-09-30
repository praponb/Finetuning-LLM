WITH Numbered AS (
    SELECT *, RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) row_num
    FROM sessions
)
SELECT id, browser, started_at
FROM Numbered
WHERE row_num <= 3;

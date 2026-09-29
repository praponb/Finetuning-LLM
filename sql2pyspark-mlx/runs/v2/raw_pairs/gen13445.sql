WITH Src AS (
    SELECT *
    FROM stores
    WHERE YEAR(opened_date) = 2023 AND region <> 'East' AND sqft <= 8366
),
StoresStats AS (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, MAX(opened_date) AS last_opened_date
    FROM Src
    GROUP BY DATE_TRUNC('MONTH', opened_date)
)
SELECT *
FROM StoresStats;

WITH Src AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE city = 'Tokyo' AND (opened_date <= '2024-04-01' OR LENGTH(store_name) < 12)
),
agg AS (
    SELECT MONTH(opened_date) AS opened_month, COUNT(sqft) AS sqft_count, MAX(opened_date) AS latest_opened_date
    FROM Src
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, latest_opened_date
FROM agg
ORDER BY latest_opened_date, opened_month
LIMIT 50

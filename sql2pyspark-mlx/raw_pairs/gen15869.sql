WITH valid_accounts AS (
    SELECT *, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months
    FROM accounts
    WHERE YEAR(opened_date) >= 2025 OR customer_id IN (11, 20, 39)
),
agg AS (
    SELECT YEAR(opened_date) AS opened_year, ROUND(AVG(balance), 2) AS avg_balance
    FROM valid_accounts
    GROUP BY YEAR(opened_date)
)
SELECT *
FROM agg;

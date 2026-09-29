WITH Src AS (
    SELECT *
    FROM store_sales
    WHERE units >= 35 AND MONTH(sale_date) IN (6, 9, 11)
),
sale_year_summary AS (
    SELECT YEAR(sale_date) sale_year, MAX(sale_date) latest_sale_date
    FROM Src
    GROUP BY YEAR(sale_date)
)
SELECT sale_year, latest_sale_date
FROM sale_year_summary
ORDER BY sale_year DESC, latest_sale_date

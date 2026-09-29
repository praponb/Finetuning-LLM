SELECT YEAR(sale_date) sale_year, QUARTER(sale_date) sale_quarter, COUNT(*) row_count
FROM store_sales
WHERE sale_date <= '2025-02-01'
GROUP BY 1, 2
ORDER BY sale_year DESC, sale_quarter
LIMIT 5

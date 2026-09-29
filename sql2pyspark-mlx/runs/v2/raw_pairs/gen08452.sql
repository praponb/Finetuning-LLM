SELECT *
FROM (
    SELECT QUARTER(sale_date) AS sale_quarter, MAX(sale_date) AS latest_sale_date, SUM(units) AS total_units
    FROM store_sales
    GROUP BY QUARTER(sale_date)
) AS agg
WHERE total_units > 130
ORDER BY sale_quarter ASC;

SELECT launch_month, avg_price, unique_categorys
FROM (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, AVG(price) AS avg_price, COUNT(DISTINCT cost) AS unique_categorys
    FROM products
    WHERE price < 670
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
) AS sub
WHERE unique_categorys < 7;

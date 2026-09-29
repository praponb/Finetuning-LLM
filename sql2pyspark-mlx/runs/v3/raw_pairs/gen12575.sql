SELECT YEAR(opened_date) AS opened_year, AVG(sqft) AS avg_sqft FROM stores WHERE sqft BETWEEN 7244 AND 15637 AND opened_date BETWEEN '2023-08-01' AND '2025-02-04' GROUP BY YEAR(opened_date);

SELECT id, region, opened_date, ROUND(sqft, 0) AS sqft_rounded, MONTH(opened_date) AS opened_date_month FROM stores WHERE opened_date BETWEEN '2025-08-27' AND '2025-10-01' ORDER BY 5;

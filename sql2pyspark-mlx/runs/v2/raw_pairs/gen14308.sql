SELECT MONTH(opened_date) AS opened_month, MIN(opened_date) AS min_opened_date
FROM stores
WHERE opened_date > '2025-06-13' AND sqft > 7784
GROUP BY MONTH(opened_date);

SELECT city, SUM(age) AS total_age, COUNT(*) AS cnt, ROUND(SUM(age) * 100.0 / SUM(SUM(age)) OVER (), 2) AS pct_of_total FROM customers WHERE age <= 49 GROUP BY city;

SELECT end_date, plan, WEEKOFYEAR(start_date) AS start_date_weekofyear
FROM subscriptions
WHERE monthly_fee BETWEEN 150 AND 180 AND plan IN ('enterprise', 'basic', 'premium')
ORDER BY end_date;

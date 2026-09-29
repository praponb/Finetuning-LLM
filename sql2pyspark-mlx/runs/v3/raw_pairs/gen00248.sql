SELECT plan, CASE WHEN monthly_fee >= 241 THEN 'High' WHEN monthly_fee >= 86 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level FROM subscriptions WHERE end_date BETWEEN '2024-08-11' AND '2024-12-01';

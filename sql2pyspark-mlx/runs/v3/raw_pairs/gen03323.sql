SELECT monthly_fee AS value FROM subscriptions WHERE end_date BETWEEN '2023-08-01' AND '2024-01-16' UNION SELECT amount AS value FROM payments ORDER BY value;

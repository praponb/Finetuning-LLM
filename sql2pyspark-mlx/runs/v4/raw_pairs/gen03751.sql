SELECT monthly_fee AS value FROM subscriptions WHERE end_date < '2025-10-01' INTERSECT SELECT amount AS value FROM orders ORDER BY value;

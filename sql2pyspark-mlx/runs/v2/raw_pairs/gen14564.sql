SELECT end_date, customer_id FROM subscriptions WHERE monthly_fee BETWEEN 170 AND 190 UNION ALL SELECT end_date, customer_id FROM subscriptions WHERE end_date > '2025-09-01';

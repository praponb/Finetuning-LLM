SELECT id, start_date, status, plan FROM subscriptions WHERE start_date BETWEEN '2023-12-01' AND '2025-01-13' AND monthly_fee BETWEEN 160 AND 240 AND plan = 'basic';

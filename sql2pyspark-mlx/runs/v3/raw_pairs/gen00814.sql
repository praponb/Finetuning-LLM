SELECT id, end_date, monthly_fee, customer_id, INITCAP(TRIM(plan)) AS plan_title FROM subscriptions WHERE end_date BETWEEN '2024-04-03' AND '2025-10-23';

SELECT plan, monthly_fee, customer_id, LOWER(plan) AS plan_lower FROM subscriptions WHERE end_date > '2025-10-01';

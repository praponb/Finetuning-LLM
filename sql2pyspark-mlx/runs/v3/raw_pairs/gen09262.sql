SELECT id, name, ROUND(age / credit_limit, 2) AS age_per_credit_limit, LOWER(name) AS name_lower FROM customers WHERE segment IN ('SMB', 'Consumer') OR signup_date < '2025-08-29' ORDER BY id DESC

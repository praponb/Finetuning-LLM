SELECT amount, SUBSTRING(status, 1, 4) AS status_prefix FROM payments WHERE amount > 730 AND amount BETWEEN 730 AND 1180 AND paid_at < '2024-07-09'

SELECT *, COALESCE(email, 'unassigned') AS email_filled, COALESCE(email, 'unassigned') AS email_filled_2
FROM customers
WHERE credit_limit >= 9500

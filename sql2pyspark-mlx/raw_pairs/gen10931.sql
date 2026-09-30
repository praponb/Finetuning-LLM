SELECT id, credit_limit, LAST_DAY(signup_date) signup_date_month_end
FROM customers
WHERE segment NOT IN ('SMB', 'Consumer') AND NOT is_active;

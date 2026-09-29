SELECT QUARTER(signup_date) AS signup_quarter, SUM(CASE WHEN segment = 'SMB' THEN age ELSE 0 END) AS smb_age, MIN(signup_date) AS first_signup_date
FROM customers
WHERE segment IN ('Consumer', 'SMB', 'Corporate')
GROUP BY signup_quarter
HAVING COUNT(*) >= 1
ORDER BY first_signup_date DESC

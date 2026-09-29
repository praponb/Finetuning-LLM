SELECT CASE WHEN credit_limit >= 15700 THEN 'high' WHEN credit_limit >= 3900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SUM(CASE WHEN segment = 'SMB' THEN age ELSE 0 END) AS smb_age, MIN(signup_date) AS first_signup_date
FROM customers
WHERE segment IN ('Consumer', 'SMB', 'Corporate')
GROUP BY credit_limit_bucket
HAVING COUNT(*) >= 1
ORDER BY first_signup_date DESC

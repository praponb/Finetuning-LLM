SELECT segment, MIN(signup_date) min_signup_date
FROM customers
WHERE segment IN ('Consumer', 'SMB', 'Corporate') AND segment IN ('Corporate', 'SMB', 'Consumer')
GROUP BY segment
ORDER BY segment
LIMIT 20

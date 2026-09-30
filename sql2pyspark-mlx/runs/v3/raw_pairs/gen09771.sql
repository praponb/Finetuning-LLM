SELECT *, CONCAT_WS('-', segment, country) AS segment_country_key
FROM customers
WHERE segment IN ('SMB', 'Consumer', 'Corporate') OR name LIKE 'Jac%';

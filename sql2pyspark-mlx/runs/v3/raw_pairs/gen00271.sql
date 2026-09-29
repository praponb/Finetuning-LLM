SELECT *, CONCAT_WS('-', name, city) AS name_city_key
FROM customers
WHERE segment IN ('Corporate', 'SMB', 'Consumer') AND segment IN ('SMB', 'Consumer', 'Corporate')
ORDER BY segment;

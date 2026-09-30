SELECT country
FROM suppliers
EXCEPT
SELECT country
FROM sessions
WHERE device <> 'mobile'

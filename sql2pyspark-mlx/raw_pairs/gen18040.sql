SELECT country FROM suppliers UNION SELECT country FROM sessions WHERE device IN ('desktop', 'mobile') ORDER BY country DESC;

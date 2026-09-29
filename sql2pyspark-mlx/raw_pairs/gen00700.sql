SELECT s.browser, c.age FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id WHERE s.browser IN ('Firefox', 'Edge') AND c.age IS NOT NULL ORDER BY age DESC;

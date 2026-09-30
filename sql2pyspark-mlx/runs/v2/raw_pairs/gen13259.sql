SELECT c.id, c.country, c.credit_limit FROM customers c WHERE c.credit_limit > (SELECT AVG(c2.credit_limit) FROM customers c2 WHERE c2.country = c.country) ORDER BY c.credit_limit DESC;

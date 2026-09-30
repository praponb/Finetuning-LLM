SELECT a.id, a.country, a.credit_limit, a.is_active FROM customers a WHERE a.credit_limit > (SELECT AVG(b.credit_limit) FROM customers b WHERE b.country = a.country)

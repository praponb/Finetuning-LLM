SELECT a.id, a.country, a.credit_limit, a.age FROM customers a WHERE a.credit_limit = (SELECT MAX(b.credit_limit) FROM customers b WHERE b.country = a.country) ORDER BY a.id DESC, a.credit_limit DESC

select a.id, a.country, a.credit_limit, a.city from customers a where a.credit_limit = (select min(b.credit_limit) from customers b where b.country = a.country)

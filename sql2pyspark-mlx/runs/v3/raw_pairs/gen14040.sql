select a.id, a.country, a.credit_limit, a.signup_date from customers a where a.credit_limit = (select max(b.credit_limit) from customers b where b.country = a.country) order by a.id desc;

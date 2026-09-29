select id, credit_limit from customers where credit_limit > (select avg(credit_limit) from customers where length(email) > 5)

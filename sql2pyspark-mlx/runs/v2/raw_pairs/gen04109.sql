select id, credit_limit from customers where credit_limit < (select min(credit_limit) from customers);

select id, signup_date, credit_limit, lead(credit_limit, 1) over (order by signup_date, id) as next_credit_limit from customers where signup_date > '2023-11-01';

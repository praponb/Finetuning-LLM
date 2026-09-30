select id, signup_date, is_active, city, greatest(credit_limit, age) greatest_credit_limit_age from customers where name like 'No%' order by signup_date asc, id asc limit 20;

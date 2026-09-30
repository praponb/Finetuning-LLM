select id, age, signup_date
from customers
where age >= (select max(age) from customers)
order by id desc, signup_date;

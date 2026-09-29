select id, age, is_active from customers where age > (select avg(age) from customers where country in ('JP', 'US'))

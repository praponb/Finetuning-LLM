select id, age from customers where age < (select min(age) from customers) order by id desc, age desc;

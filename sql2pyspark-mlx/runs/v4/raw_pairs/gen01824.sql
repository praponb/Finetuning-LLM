select id, age from customers where age > (select avg(age) from customers) order by age desc;

select c.id, c.country, c.age from customers c where c.age = (select max(c2.age) from customers c2 where c2.country = c.country)

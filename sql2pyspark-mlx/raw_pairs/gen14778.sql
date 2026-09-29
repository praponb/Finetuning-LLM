select id, city, age, row_number() over (partition by city order by age desc, id) as rn from customers where email is not null

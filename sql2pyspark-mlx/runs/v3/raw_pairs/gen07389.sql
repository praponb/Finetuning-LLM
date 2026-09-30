select id, city, signup_date, age, lead(age, 1) over (partition by city order by signup_date, id) as next_age from customers

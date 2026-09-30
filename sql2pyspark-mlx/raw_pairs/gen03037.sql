select id, city, signup_date, rank() over (partition by city order by signup_date) as rnk from customers where email like '%@gmail.com' order by id

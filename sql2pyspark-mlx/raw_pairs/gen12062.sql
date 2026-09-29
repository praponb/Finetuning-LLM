select id, name, credit_limit, signup_date, upper(city) as city_upper from customers where age = 36 order by name asc, id asc limit 20

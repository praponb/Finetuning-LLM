select id, city, country, age, avg(age) over (partition by city, country) as city_avg_age from customers where credit_limit between 7700 and 9000

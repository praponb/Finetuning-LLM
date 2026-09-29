select * from (select country, city, credit_limit from customers) pivot (count(credit_limit) for city in ('Singapore', 'Chiang Mai', 'New York', 'London')) order by country;

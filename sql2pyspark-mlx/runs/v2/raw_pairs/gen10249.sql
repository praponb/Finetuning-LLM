select * from (select country, city, credit_limit from customers)
pivot (max(credit_limit) for city in ('London', 'Berlin', 'Singapore', 'Tokyo'))
order by country;

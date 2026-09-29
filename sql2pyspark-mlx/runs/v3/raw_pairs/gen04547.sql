select id, city, email, country, concat(name, '/', country) as name_country from customers where credit_limit between 6900 and 10500 or email like '%@startup.io' order by city, id limit 50;

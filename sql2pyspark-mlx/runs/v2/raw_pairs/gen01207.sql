select * from (select city, country, age from customers) pivot (max(age) for country in ('UK', 'SG', 'JP')) order by city;

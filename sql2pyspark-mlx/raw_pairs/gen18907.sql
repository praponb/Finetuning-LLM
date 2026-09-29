select city, round(avg(age), 2) as avg_age from customers where city in ('Tokyo', 'New York', 'London') and credit_limit >= 10700 group by city order by avg_age desc, city desc limit 20

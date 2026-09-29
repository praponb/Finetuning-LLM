select * from (select segment, country, age from customers)
pivot (count(age) for country in ('TH', 'SG', 'DE'));

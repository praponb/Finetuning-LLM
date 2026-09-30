select * from (select case when age >= 73 then 'high' when age >= 40 then 'mid' else 'low' end as age_bucket, city, age from customers) pivot (avg(age) for city in ('London', 'Singapore'))

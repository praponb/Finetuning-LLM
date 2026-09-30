select id, country, segment, age, sum(age) over (partition by country, segment) as country_sum_age from customers order by id desc;

with top_customers as (select id, city, age, row_number() over (partition by city order by age desc, id) as rnk from customers) select * from top_customers where rnk <= 3 order by city

with top_customers as (select id, city, signup_date, row_number() over (partition by city order by signup_date desc, id) as pos from customers) select * from top_customers where pos <= 3;

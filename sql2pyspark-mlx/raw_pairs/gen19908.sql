with ranked_customers as (select *, row_number() over (partition by country order by age desc, id) as rn from customers) select id, country, age from ranked_customers where rn <= 3

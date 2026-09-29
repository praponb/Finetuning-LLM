with ranked_customers as (select *, row_number() over (partition by city order by age desc, id) as rnk from customers) select id, city, age, credit_limit from ranked_customers where rnk = 1;

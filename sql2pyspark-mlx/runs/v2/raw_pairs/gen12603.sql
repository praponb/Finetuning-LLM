with ranked_customers as (select *, rank() over (partition by segment order by signup_date desc) as rn from customers) select id, segment, signup_date, email from ranked_customers where rn <= 3

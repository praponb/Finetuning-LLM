select city, sum(credit_limit) as total_credit_limit, count(*) as cnt, dense_rank() over (order by sum(credit_limit) desc) as rnk from customers group by city

select merchant, sum(amount) as total_amount, count(*) as cnt, rank() over (order by sum(amount) desc) as rnk from transactions group by merchant

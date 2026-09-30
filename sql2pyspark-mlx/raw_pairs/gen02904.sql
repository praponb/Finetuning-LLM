select merchant, sum(amount) as total_amount, count(*) as cnt, sum(sum(amount)) over (order by merchant) as cumulative_amount from transactions group by merchant

select city, sum(credit_limit) as total_credit_limit, count(*) as cnt, sum(sum(credit_limit)) over (order by city) as cumulative_credit_limit from customers group by city order by city limit 10;

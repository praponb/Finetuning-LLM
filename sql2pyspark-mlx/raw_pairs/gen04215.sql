select channel, sum(amount) as total_amount, dense_rank() over (order by sum(amount) desc) as rnk from orders group by channel;

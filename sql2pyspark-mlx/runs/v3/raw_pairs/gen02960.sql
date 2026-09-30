select hour(paid_at) as paid_hour, sum(amount) as total_amount, dense_rank() over (order by sum(amount) desc) as amount_rank from payments group by hour(paid_at) order by paid_hour desc limit 10;

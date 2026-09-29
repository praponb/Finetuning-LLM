select order_id, sum(amount) total_amount, count(*) cnt, dense_rank() over (order by sum(amount) desc) as amount_rank from payments where status in ('SUCCESS', 'FAILED') group by order_id;

select
    is_flagged,
    sum(amount) total_amount,
    count(*) cnt,
    dense_rank() over (order by sum(amount) desc) as rnk
from transactions
group by is_flagged
order by rnk desc, is_flagged
limit 5;

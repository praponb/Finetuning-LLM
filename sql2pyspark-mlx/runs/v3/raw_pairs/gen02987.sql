select *
from (
    select channel, status, sum(amount) as total_amount
    from orders
    where store_id is not null
    group by channel, status
) as sub
where total_amount < 9300
order by total_amount desc, channel;

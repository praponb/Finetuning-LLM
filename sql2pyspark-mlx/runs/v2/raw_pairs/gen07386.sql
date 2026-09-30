select *
from (
    select channel, max(discount) as max_discount
    from orders
    where discount is not null
    group by channel
) as x
where max_discount > 0.34
order by max_discount desc;

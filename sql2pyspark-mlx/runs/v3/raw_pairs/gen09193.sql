with src as (
    select *
    from orders
    where store_id is not null
),
summary as (
    select channel, round(stddev(discount), 2) as stddev_discount, sort_array(collect_set(channel)) as channel_list
    from src
    group by channel
)
select *
from summary
where stddev_discount > 67
order by channel;

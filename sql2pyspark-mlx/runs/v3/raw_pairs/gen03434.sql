select channel, count(case when channel = 'partner' then 1 end) as partner_count, concat_ws(', ', sort_array(collect_set(channel))) as channel_values
from orders
where channel = 'partner'
group by channel;

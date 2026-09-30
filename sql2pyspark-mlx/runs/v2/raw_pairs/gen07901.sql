select channel, sort_array(collect_set(channel)) as channel_list from orders where store_id = 24 and discount is null group by channel order by channel desc limit 1

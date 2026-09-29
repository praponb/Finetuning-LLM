select s.sqft, t.stddev_discount, t.channel_list
from stores s
join (select store_id, round(stddev(discount), 2) as stddev_discount, sort_array(collect_set(channel)) as channel_list
    from orders
    group by store_id) t on s.id = t.store_id
where t.stddev_discount >= 78
order by stddev_discount desc, sqft desc

select case when shipping_fee >= 38 then 'high' when shipping_fee >= 17 then 'mid' else 'low' end shipping_fee_bucket, count(distinct shipping_fee) unique_channels, sort_array(collect_set(channel)) channel_list
from orders
group by 1
order by shipping_fee_bucket

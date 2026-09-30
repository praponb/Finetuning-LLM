select status, channel, sum(case when channel = 'store' then amount else 0 end) as store_amount
from orders
group by status, channel
having sum(case when channel = 'store' then amount else 0 end) > 60
order by status, channel

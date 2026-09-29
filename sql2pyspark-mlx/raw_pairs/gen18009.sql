select o.channel, min(s.shipped_date) as first_shipped_date
from shipments s
full outer join orders o on o.id = s.order_id
group by o.channel

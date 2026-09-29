select order_id, count(case when status = 'LOST' then 1 end) as lost_count, max(shipped_date) as last_shipped_date
from shipments
group by 1

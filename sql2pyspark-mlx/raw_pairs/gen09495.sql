select status, sum(case when status = 'DELIVERED' then 1 else 0 end) as delivered_count
from shipments
where status = 'LOST'
group by status
order by status;

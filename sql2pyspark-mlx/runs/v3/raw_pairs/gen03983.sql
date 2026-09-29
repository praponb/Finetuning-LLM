select carrier, sum(case when status = 'LOST' then cost else 0 end) as lost_cost
from shipments
group by carrier
order by carrier, lost_cost desc;

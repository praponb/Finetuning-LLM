select carrier, sum(case when status = 'RETURNED' then cost else 0 end) as returned_cost, min(shipped_date) as min_shipped_date
from shipments
group by carrier
order by carrier, returned_cost desc;

select
    status,
    case when cost >= 89 then 'high' when cost >= 33 then 'mid' else 'low' end cost_bucket,
    min(shipped_date) first_shipped_date,
    count(distinct cost) unique_carriers
from shipments
where month(delivered_date) in (7, 11, 12)
group by 1, 2
having unique_carriers >= 5;

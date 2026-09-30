select
    i.id,
    i.store_id,
    i.quantity,
    s.sqft,
    s.region,
    s.opened_date,
    case when s.sqft >= 12906 then 'High' when s.sqft >= 7370 then 'Medium' else 'Low' end as sqft_band
from inventory i
join stores s on i.store_id = s.id
order by store_id desc

select
    id,
    carrier,
    status,
    case when status = 'LOST' then 1 else 0 end as is_lost
from shipments
where month(delivered_date) in (1, 7, 8);

select
    id,
    device,
    customer_id,
    case when country = 'SG' then 1 else 0 end as is_sg,
    to_date(started_at) as started_at_date
from sessions
where browser = 'Safari'
order by device

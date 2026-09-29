select
    id,
    customer_id,
    opened_date,
    last_day(opened_date) as opened_date_month_end,
    case when status = 'CLOSED' then 1 else 0 end as is_closed
from accounts
where status = 'FROZEN' and (year(opened_date) = 2025 or status in ('FROZEN', 'CLOSED', 'OPEN'))
order by id desc
limit 10;

select
    id,
    status,
    balance,
    opened_date,
    first_value(opened_date) over (partition by status order by balance desc, id) as first_opened_date
from accounts
where status is not null
order by status

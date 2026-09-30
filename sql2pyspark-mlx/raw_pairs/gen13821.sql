with Numbered as (
    select *, row_number() over (partition by status order by balance, id) as rnk
    from accounts
)
select
    id,
    status,
    balance,
    opened_date
from Numbered
where rnk <= 3
order by status

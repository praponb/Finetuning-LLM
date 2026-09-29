with numbered as (
    select *, row_number() over (partition by account_type order by opened_date desc, id) pos
    from accounts
)
select id, account_type, opened_date
from numbered
where pos <= 3
order by account_type;

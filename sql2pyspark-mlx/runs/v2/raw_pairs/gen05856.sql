with RankedAccounts as (
    select *, row_number() over (partition by account_type order by opened_date desc, id) as rn
    from accounts
)
select id, account_type, opened_date
from RankedAccounts
where rn = 1
order by id, opened_date desc;

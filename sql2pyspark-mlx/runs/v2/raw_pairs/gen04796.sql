with top_accounts as (
    select *, row_number() over (partition by account_type order by opened_date desc, id) as pos
    from accounts
)
select *
from top_accounts
where pos <= 1
order by opened_date desc;

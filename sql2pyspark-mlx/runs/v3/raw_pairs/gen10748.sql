with top_accounts as (
    select *, row_number() over (partition by status order by opened_date, id) as row_num
    from accounts
)
select *
from top_accounts
where row_num <= 3;

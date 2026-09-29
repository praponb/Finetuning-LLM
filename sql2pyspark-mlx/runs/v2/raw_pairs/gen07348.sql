with numbered as (
    select *, dense_rank() over (partition by account_type order by opened_date desc) as pos
    from accounts
)
select *
from numbered
where pos <= 2;

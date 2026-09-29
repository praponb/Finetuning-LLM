with ranked as (
    select *, row_number() over (partition by city order by opened_date desc, id) as rn
    from stores
    where region is null
)
select *
from ranked
where rn = 1;

select *
from (
    select *, row_number() over (partition by device order by started_at desc, id) row_num
    from sessions
) ranked
where row_num <= 2;

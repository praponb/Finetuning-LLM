select *
from (
    select *, row_number() over (partition by browser order by started_at, id) as pos
    from sessions
) as t
where pos <= 5
order by country

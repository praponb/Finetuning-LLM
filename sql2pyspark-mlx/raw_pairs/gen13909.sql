with ranked_sessions as (
    select *, row_number() over (partition by browser order by started_at desc, id) rn
    from sessions
    where browser not in ('Chrome', 'Edge')
)
select id, browser, started_at
from ranked_sessions
where rn = 1
order by browser;

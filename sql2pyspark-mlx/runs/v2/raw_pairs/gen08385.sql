with top_sessions as (
    select *, rank() over (partition by browser order by started_at desc) as row_num
    from sessions
)
select id, browser, started_at
from top_sessions
where row_num <= 2
order by 2 asc

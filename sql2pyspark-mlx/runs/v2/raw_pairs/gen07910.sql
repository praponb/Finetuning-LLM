with top_sessions as (select *, rank() over (partition by device order by started_at desc) as rn from sessions) select * from top_sessions where rn = 1 order by browser

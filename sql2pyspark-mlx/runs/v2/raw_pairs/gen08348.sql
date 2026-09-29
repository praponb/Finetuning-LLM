with ranked_sessions as (select *, row_number() over (partition by device order by started_at desc, id) as pos from sessions) select id, device, started_at from ranked_sessions where pos <= 5;

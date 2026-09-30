select id, started_at, row_number() over (order by started_at, id) row_num from sessions;

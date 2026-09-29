select id, country, started_at, lead(started_at, 2) over (partition by country order by started_at, id) as next_started_at from sessions

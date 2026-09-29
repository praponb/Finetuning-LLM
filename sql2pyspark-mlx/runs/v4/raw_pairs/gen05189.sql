select id, country, started_at, ntile(3) over (partition by country order by started_at, id) as started_at_ntile from sessions

select id, browser, started_at, lead(started_at, 1) over (partition by browser order by started_at, id) as next_started_at from sessions

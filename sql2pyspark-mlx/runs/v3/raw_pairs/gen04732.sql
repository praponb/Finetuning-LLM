select id, device, started_at, rank() over (partition by device order by started_at desc) as rnk, ntile(2) over (partition by device order by started_at desc, id) as quartile from sessions

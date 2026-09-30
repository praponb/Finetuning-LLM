select id, device, started_at, lead(started_at, 1) over (partition by device order by started_at, id) next_started_at from sessions where country = 'TH';

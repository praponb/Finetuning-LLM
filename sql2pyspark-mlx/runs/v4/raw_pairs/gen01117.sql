select id, device, started_at, row_number() over (partition by device order by started_at desc, id) as rn from sessions where country != 'DE';

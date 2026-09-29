with numbered as (select id, device, started_at, browser, row_number() over (partition by device order by started_at desc, id) as pos from sessions) select * from numbered where pos <= 2 order by pos

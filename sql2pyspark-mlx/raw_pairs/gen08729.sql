select * from (select id, device, started_at, browser, row_number() over (partition by device order by started_at desc, id) row_num from sessions) t where row_num <= 5 order by id desc

select id, device, started_at, row_number() over (partition by device order by started_at, id) as row_num from sessions where customer_id in (8, 9, 27, 35)

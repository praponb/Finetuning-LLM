select id, status, opened_date, row_number() over (partition by status order by opened_date, id) as row_num from accounts where month(opened_date) in (2, 9, 11)

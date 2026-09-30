select id, status, balance from (select *, row_number() over (partition by status order by balance desc, id) as row_num from accounts where status = 'FROZEN') as t where row_num <= 3 order by id desc

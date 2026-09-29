select id, amount, status from (select *, row_number() over (partition by method, status order by amount desc, id) as row_num from payments) x where row_num = 1;

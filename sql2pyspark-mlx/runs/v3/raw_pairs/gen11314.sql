select * from (select *, row_number() over (partition by account_type order by opened_date desc, id) as row_num from accounts) as r where row_num = 1 order by account_type, row_num desc;

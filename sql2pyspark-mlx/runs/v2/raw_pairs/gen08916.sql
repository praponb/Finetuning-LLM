select * from (select *, row_number() over (partition by txn_type order by amount desc, id) row_num from transactions) r where row_num <= 3;

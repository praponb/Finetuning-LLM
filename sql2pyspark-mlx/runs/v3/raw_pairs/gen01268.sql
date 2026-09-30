select * from (select *, row_number() over (partition by txn_type order by txn_time desc, id) as row_num from transactions) as ranked where row_num <= 2

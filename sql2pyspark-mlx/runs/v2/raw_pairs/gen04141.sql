select * from (select *, row_number() over (partition by merchant order by txn_time, id) as rn from transactions where txn_time >= '2024-09-23') as r where rn = 1 order by amount desc

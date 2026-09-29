select * from (select *, row_number() over (partition by txn_type order by txn_time desc, id) as rnk from transactions where merchant in ('Lazada', 'Starbucks', 'Grab')) as r where rnk <= 3

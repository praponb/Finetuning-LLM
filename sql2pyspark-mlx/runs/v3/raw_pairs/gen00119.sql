with numbered as (select *, dense_rank() over (partition by merchant order by txn_time) as rnk from transactions) select id, merchant, txn_time, txn_type from numbered where rnk <= 3

select * from (select *, row_number() over (partition by account_type, customer_id order by opened_date asc nulls last, id asc) as dedup_rank from accounts) x where dedup_rank = 1

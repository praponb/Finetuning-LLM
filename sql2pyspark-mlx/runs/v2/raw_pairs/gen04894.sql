with Ranked as (select *, row_number() over (partition by txn_type order by txn_time desc, id) as pos from transactions) select id, txn_type, txn_time from Ranked where pos <= 3

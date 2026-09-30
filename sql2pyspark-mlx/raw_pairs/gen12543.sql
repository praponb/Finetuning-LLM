select txn_type, grab_count, credit_count
from (
    select txn_type, sum(case when merchant = 'Grab' then 1 else 0 end) as grab_count, sum(case when txn_type = 'CREDIT' then 1 else 0 end) as credit_count
    from transactions
    group by txn_type
) as x
where grab_count >= 23
order by credit_count desc;

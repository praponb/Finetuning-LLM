select merchant, refund_count
from (
    select merchant, count(case when txn_type = 'REFUND' then 1 end) as refund_count
    from transactions
    group by merchant
) as x
where refund_count > 3
order by refund_count desc, merchant
limit 10;

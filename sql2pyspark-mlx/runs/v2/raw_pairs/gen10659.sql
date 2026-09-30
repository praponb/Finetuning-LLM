select
    txn_type,
    is_flagged,
    case when amount >= 4300 then 'high' when amount >= 900 then 'mid' else 'low' end as amount_bucket,
    count(*) as num_transactions
from transactions
where account_id in (21, 35) and to_date(txn_time) < '2024-07-01'
group by txn_type, is_flagged, case when amount >= 4300 then 'high' when amount >= 900 then 'mid' else 'low' end
order by amount_bucket;

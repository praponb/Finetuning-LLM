select
    amount_bucket,
    is_flagged,
    amount_range,
    txn_type_list
from (
    select
        case when amount >= 4000 then 'high' when amount >= 1900 then 'mid' else 'low' end as amount_bucket,
        is_flagged,
        max(amount) - min(amount) as amount_range,
        sort_array(collect_set(txn_type)) as txn_type_list
    from transactions
    where to_date(txn_time) >= '2023-04-01'
    group by case when amount >= 4000 then 'high' when amount >= 1900 then 'mid' else 'low' end, is_flagged
) as sub
where amount_range >= 62;

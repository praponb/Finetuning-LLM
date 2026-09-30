select
    account_id,
    sum(amount) as total_amount,
    min(amount) as min_amount,
    concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
from transactions
where txn_type in ('CREDIT', 'REFUND', 'FEE')
group by account_id
order by min_amount

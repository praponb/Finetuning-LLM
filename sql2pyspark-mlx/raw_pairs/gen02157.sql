with transactions_by_account as (
    select account_id, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values, min(txn_time) as min_txn_time
    from transactions
    where merchant in ('Lazada', 'Shell')
    group by account_id
)
select a.opened_date, tba.merchant_values, tba.min_txn_time
from accounts a
join transactions_by_account tba on a.id = tba.account_id
order by tba.min_txn_time desc

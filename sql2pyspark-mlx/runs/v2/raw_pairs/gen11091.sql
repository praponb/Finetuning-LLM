with account_stats as (
    select account_id, concat_ws(', ', sort_array(collect_set(merchant))) merchant_values, count(distinct amount) unique_merchants
    from transactions
    where hour(txn_time) between 7 and 11
    group by account_id
)
select accounts.opened_date, account_stats.merchant_values, account_stats.unique_merchants
from accounts
left outer join account_stats on accounts.id = account_stats.account_id
where account_stats.unique_merchants >= 7
order by accounts.opened_date, merchant_values;

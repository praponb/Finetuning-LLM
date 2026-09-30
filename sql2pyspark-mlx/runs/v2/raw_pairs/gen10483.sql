with transactions_by_account as (
    select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values, round((100 * sum(case when merchant = '7-Eleven' then 1 else 0 end)) / count(*), 1) as pct_7_eleven
    from transactions
    group by account_id
)
select
    accounts.balance,
    accounts.account_type,
    transactions_by_account.txn_type_values,
    transactions_by_account.pct_7_eleven
from accounts
left join transactions_by_account on accounts.id = transactions_by_account.account_id
where transactions_by_account.pct_7_eleven > 37
order by balance desc

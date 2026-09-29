with account_totals as (
    select account_id, count(case when txn_type = 'DEBIT' then 1 end) as debit_count
    from transactions
    where is_flagged = false
    group by account_id
)
select accounts.id, accounts.opened_date, account_totals.debit_count
from accounts
left join account_totals on accounts.id = account_totals.account_id
where account_totals.debit_count > 9
order by debit_count desc, id;

with account_stats as (
    select account_id, round((100 * sum(case when txn_type = 'CREDIT' then 1 else 0 end)) / count(*), 1) as pct_credit
    from transactions
    group by account_id
)
select a.account_type, a.opened_date, as.pct_credit
from accounts a
inner join account_stats as on a.id = as.account_id
where as.pct_credit >= 57
order by a.account_type

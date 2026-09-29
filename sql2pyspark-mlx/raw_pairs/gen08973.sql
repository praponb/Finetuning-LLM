select
    a.id,
    a.balance,
    a.opened_date,
    t.credit_count,
    t.pct_shell
from accounts a
left outer join (select account_id, sum(case when txn_type = 'CREDIT' then 1 else 0 end) as credit_count, round((100 * sum(case when merchant = 'Shell' then 1 else 0 end)) / count(*), 1) as pct_shell
    from transactions
    where merchant in ('7-Eleven', 'Grab', 'Shell')
    group by account_id) t on a.id = t.account_id
order by pct_shell desc;

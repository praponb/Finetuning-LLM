select
    a.status,
    count(t.id) as transaction_count,
    round((100 * sum(case when t.txn_type = 'DEBIT' then 1 else 0 end)) / count(*), 1) as pct_debit,
    avg(t.amount) as avg_amount
from accounts a
left join transactions t on t.account_id = a.id
where t.amount < 1500
group by a.status
order by pct_debit desc, status;

select a.status, count(case when t.txn_type = 'DEBIT' then 1 end) as debit_count, count(distinct t.amount) as unique_accounts
from transactions t
right join accounts a on t.account_id = a.id
where a.opened_date < '2025-10-01'
group by a.status
order by status
limit 20;

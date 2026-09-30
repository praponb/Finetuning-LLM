select a.status, count(distinct t.amount) as unique_accounts, count(*) as num_transactions
from transactions t
left join accounts a on t.account_id = a.id
where a.opened_date > '2025-06-01'
group by a.status
order by status

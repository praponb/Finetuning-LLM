select quarter(a.opened_date) as opened_quarter, round(avg(t.amount), 2) as avg_amount, max(t.txn_time) as latest_txn_time
from transactions t
inner join accounts a on t.account_id = a.id
where a.status = 'OPEN'
group by quarter(a.opened_date)
order by avg_amount, latest_txn_time;

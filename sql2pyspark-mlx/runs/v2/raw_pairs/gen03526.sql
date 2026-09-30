select year(a.opened_date) as opened_year, sum(t.amount) as total_amount, count(distinct t.amount) as unique_txn_types
from transactions t
right join accounts a on t.account_id = a.id
group by year(a.opened_date)
having count(distinct t.amount) >= 1
order by total_amount desc;

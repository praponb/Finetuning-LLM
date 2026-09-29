select a.status, sort_array(collect_set(t.txn_type)) as txn_type_list, sum(case when t.txn_type = 'FEE' then t.amount else 0 end) as fee_amount
from transactions t
inner join accounts a on a.id = t.account_id
where a.opened_date < '2023-06-01'
group by a.status
order by status
limit 10;

select a.status, count(case when t.merchant = 'Lazada' then 1 end) as lazada_count, concat_ws(', ', sort_array(collect_set(t.txn_type))) as txn_type_values
from transactions t
right outer join accounts a on t.account_id = a.id
group by a.status
order by txn_type_values, status
limit 1

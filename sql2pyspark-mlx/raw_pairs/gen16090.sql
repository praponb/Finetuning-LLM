select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values, sum(case when txn_type = 'REFUND' then 1 else 0 end) as refund_count
from transactions
where merchant = '7-Eleven'
group by account_id
having refund_count < 70
order by txn_type_values asc;

select
    merchant,
    sort_array(collect_set(txn_type)) as txn_type_list,
    min(amount) as min_amount,
    sum(amount) as total_amount
from transactions
group by 1
order by merchant;

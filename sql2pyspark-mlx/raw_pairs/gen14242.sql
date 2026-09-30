select txn_type, min(amount) as min_amount from transactions group by txn_type order by min_amount desc, txn_type;

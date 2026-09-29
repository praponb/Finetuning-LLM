select concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values from transactions where account_id in (6, 7, 15)

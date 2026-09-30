select customer_id, sort_array(collect_set(account_type)) as account_type_list, stddev_samp(balance) as balance_stddev, size(collect_set(status)) as num_status from accounts group by customer_id

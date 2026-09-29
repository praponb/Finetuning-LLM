select status, sort_array(collect_set(status)) as status_list, min(balance) as min_balance from accounts group by 1;

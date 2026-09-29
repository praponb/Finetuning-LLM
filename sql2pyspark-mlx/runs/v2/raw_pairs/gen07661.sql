select account_type, status, sort_array(collect_set(status)) as status_list from accounts group by account_type, status having max(balance) < 37000

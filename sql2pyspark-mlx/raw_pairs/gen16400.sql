select account_type, sort_array(collect_set(account_type)) as account_type_list, min(balance) as min_balance from accounts group by account_type order by min_balance desc;

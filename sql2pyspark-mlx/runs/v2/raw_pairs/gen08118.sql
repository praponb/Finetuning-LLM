select status, round(avg(balance), 2) as avg_balance, max(opened_date) as latest_opened_date, sort_array(collect_set(account_type)) as account_type_list from accounts group by status

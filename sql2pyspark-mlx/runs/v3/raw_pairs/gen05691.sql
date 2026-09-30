select status, account_type, sort_array(collect_set(status)) as status_list from accounts where account_type <> 'LOAN' and opened_date < '2024-07-01' group by status, account_type

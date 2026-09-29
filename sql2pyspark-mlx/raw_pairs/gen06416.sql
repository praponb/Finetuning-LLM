select sum(balance) total_balance, concat_ws(', ', sort_array(collect_set(account_type))) account_type_values
from accounts
where status = 'FROZEN' and opened_date > '2024-05-01'
order by total_balance;

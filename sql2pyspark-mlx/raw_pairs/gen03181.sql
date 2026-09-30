select sort_array(collect_set(account_type)) as account_type_list
from accounts
where customer_id = 37 and opened_date < '2023-04-01';

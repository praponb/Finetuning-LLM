select customer_id, length(status) as status_length, concat_ws('-', status, account_type) as status_account_type_key from accounts;

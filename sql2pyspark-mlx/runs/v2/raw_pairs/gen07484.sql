select *, lower(status) as status_lower from accounts where status = 'CLOSED' and account_type = 'LOAN';

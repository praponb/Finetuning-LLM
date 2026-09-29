select id, account_type, status, balance, length(status) status_length from accounts where status = 'CLOSED' and customer_id is not null;

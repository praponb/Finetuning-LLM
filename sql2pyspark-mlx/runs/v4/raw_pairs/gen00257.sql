select id, account_type, balance, customer_id, trim(status) clean_status from accounts where status in ('CLOSED', 'FROZEN') or month(opened_date) in (1, 7, 12);

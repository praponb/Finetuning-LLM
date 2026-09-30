select opened_date, account_type from accounts where account_type != 'CHECKING' union all select opened_date, account_type from accounts where customer_id in (2, 8, 17);

select id, account_type, opened_date, status, date_add(opened_date, 53) as opened_date_plus_53d from accounts order by status, opened_date desc

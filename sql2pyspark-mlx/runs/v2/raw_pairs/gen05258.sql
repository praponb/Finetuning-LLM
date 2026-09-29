select id, account_type, status, opened_date, row_number() over (partition by account_type, status order by opened_date, id) as rn from accounts where opened_date > '2025-05-01';

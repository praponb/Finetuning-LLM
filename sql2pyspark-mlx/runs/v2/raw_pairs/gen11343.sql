select id, balance, account_type from accounts where status in ('FROZEN', 'OPEN', 'CLOSED') order by id desc limit 5;

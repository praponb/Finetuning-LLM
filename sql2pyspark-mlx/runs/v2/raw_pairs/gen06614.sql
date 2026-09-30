select status, account_type, avg(balance) as avg_balance from accounts group by status, account_type;

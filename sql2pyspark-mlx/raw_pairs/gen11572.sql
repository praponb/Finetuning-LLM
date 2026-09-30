select account_type, count(*) as num_accounts, count(*) as account_count from accounts group by account_type having account_count >= 2;

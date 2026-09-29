select account_type, count(balance) as balance_count, count(distinct balance) as unique_account_types from accounts group by account_type

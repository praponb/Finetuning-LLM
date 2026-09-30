select account_type, max(balance) - min(balance) as balance_range from accounts group by 1;

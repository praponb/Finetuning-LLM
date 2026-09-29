select status, max(balance) - min(balance) as balance_range, round(stddev(balance), 2) as stddev_balance from accounts group by status

select date_trunc('MONTH', opened_date) as opened_month, max(balance) - min(balance) as balance_range, max(balance) as max_balance from accounts group by 1 order by opened_month

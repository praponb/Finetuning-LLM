select year(opened_date) as opened_year, max(balance) - min(balance) as balance_range from accounts group by 1;

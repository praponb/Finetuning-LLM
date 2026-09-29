select year(opened_date) opened_year, account_type, max(balance) - min(balance) balance_range from accounts group by opened_year, account_type order by balance_range;

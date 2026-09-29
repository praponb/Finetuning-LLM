SELECT status, balance, LAST_DAY(opened_date) opened_date_month_end FROM accounts WHERE balance < 22000 ORDER BY opened_date_month_end

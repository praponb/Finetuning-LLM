SELECT status, account_type, LAST_DAY(opened_date) AS opened_date_month_end
FROM accounts;

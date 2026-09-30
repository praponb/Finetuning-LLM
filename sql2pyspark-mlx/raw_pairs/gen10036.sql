select *, coalesce(balance, 0) as balance_filled, date_trunc('month', opened_date) as opened_date_period from accounts where status = 'CLOSED' order by account_type;

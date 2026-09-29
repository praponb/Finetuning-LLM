select date_trunc('MONTH', opened_date) as opened_month, status, count(*) as num_accounts from accounts where status in ('OPEN', 'FROZEN') group by date_trunc('MONTH', opened_date), status

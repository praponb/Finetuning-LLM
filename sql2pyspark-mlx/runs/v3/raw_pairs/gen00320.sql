select account_type, round(stddev(balance), 2) as stddev_balance, max(opened_date) as latest_opened_date from accounts where status <> 'CLOSED' group by account_type order by account_type;

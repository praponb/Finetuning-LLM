select count(amount) as amount_count, round(stddev(amount), 2) as stddev_amount from transactions where amount <= 3000 and to_date(txn_time) < '2024-01-29';

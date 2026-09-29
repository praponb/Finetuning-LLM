select status, round(stddev(balance), 2) stddev_balance, count(case when status = 'OPEN' then 1 end) open_count from accounts where customer_id is null group by 1;

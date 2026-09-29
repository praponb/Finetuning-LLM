select avg(credit_limit) as avg_credit_limit, sum(case when country = 'DE' then 1 else 0 end) as de_count, round(stddev(credit_limit), 2) as stddev_credit_limit from customers order by de_count;

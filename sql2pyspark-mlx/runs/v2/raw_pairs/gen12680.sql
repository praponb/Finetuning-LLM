select city, round(avg(credit_limit), 2) as avg_credit_limit, count(credit_limit) as credit_limit_count from customers where name like '%Jack%' group by 1 order by avg_credit_limit, city;

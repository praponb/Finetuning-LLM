select year(signup_date) as signup_year, sum(case when city = 'Singapore' then 1 else 0 end) as singapore_count from customers group by year(signup_date) having singapore_count >= 60;

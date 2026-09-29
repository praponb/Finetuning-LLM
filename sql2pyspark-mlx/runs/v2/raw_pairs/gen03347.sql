select year(signup_date) as signup_year, sum(credit_limit) as total_credit_limit, count(*) as cnt, rank() over (order by sum(credit_limit) desc) as rnk from customers group by year(signup_date)

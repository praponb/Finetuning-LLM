select date_format(signup_date, 'yyyy-MM') as signup_month, sum(age) as total_age, dense_rank() over (order by sum(age) desc) as age_rank from customers group by date_format(signup_date, 'yyyy-MM')

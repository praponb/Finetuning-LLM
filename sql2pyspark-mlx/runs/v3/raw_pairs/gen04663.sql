select is_active, date_format(signup_date, 'yyyy-MM') as signup_month, count(credit_limit) as credit_limit_count, count(*) as num_customers from customers group by 1, 2 order by signup_month desc

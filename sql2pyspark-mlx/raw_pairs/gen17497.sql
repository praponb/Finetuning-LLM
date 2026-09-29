select city, month(signup_date) as signup_month, max(age) as max_age from customers where name like '%Ford%' group by 1, 2

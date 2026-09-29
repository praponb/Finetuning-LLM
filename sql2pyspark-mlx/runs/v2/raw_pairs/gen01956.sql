select year(signup_date) signup_year, count(*) num_customers, count(*) total_count
from customers
where age > 63 and signup_date between '2024-04-01' and '2025-09-01'
group by signup_year
having num_customers > 5
order by total_count desc;

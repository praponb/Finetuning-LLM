select
    id,
    country,
    signup_date,
    dense_rank() over (partition by country order by signup_date desc) as signup_date_dense_rank
from customers
where signup_date <= '2025-09-05'
order by id asc;

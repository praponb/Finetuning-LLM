select
    category,
    sum(price) as total_price,
    count(*) as cnt,
    dense_rank() over (order by sum(price) desc) as rnk
from products
group by category
order by 4;

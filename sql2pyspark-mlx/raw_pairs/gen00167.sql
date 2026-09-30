select *
from (
    select year(order_date) as order_year, store_id, sum(amount) as total_amount
    from orders
    group by year(order_date), store_id
) as agg
where total_amount < 5200

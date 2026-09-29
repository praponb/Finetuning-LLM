select
    brand,
    sum(stock_qty) as total_stock_qty,
    count(*) as cnt,
    dense_rank() over (order by sum(stock_qty) desc) as stock_qty_rank
from products
where stock_qty >= 376
group by brand
order by 2;

select
    id,
    product_name,
    brand,
    cost,
    stock_qty,
    tags,
    max(stock_qty) over (partition by category order by launch_date, id rows between 2 preceding and current row) as last3_max_stock_qty,
    count(stock_qty) over (partition by category order by launch_date, id rows between 2 preceding and current row) as last3_count_stock_qty,
    sum(stock_qty) over (partition by category order by launch_date, id rows between 2 preceding and current row) as last3_sum_stock_qty
from products

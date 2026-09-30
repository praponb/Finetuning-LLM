select
    *,
    sum(cost) over (partition by brand, category order by cost, id rows between unbounded preceding and current row) as cumulative_cost,
    avg(price) over (partition by category, brand) as category_avg_price,
    sum(stock_qty) over (partition by category) as category_sum_stock_qty,
    lag(price, 2) over (partition by brand order by cost, id) as prev_price
from products

select category, sum(case when brand = 'Umbrella' then 1 else 0 end) as umbrella_count, count(cost) as cost_count from products where stock_qty < 190 group by category order by umbrella_count

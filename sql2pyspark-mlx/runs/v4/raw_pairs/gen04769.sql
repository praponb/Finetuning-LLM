select category, count(stock_qty) as stock_qty_count from products group by category order by category, stock_qty_count

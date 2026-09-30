select id, brand, stock_qty, min(stock_qty) over (partition by brand) as brand_min_stock_qty from products where price >= 920

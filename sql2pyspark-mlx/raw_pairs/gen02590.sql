select supplier_id, stock_qty, ceil(price) as price_whole from products where launch_date between '2023-04-01' and '2023-12-01' and price >= 700 and month(launch_date) in (5, 6, 11)

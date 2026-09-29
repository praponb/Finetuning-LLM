select brand, sum(case when category = 'Sports' then stock_qty else 0 end) as sports_stock_qty from products where year(launch_date) = 2024 or price > 570 group by brand;

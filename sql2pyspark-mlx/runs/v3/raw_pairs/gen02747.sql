select brand, max(launch_date) as latest_launch_date from products group by brand having count(distinct stock_qty) >= 3;

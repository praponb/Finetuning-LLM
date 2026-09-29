select month(launch_date) as launch_month, min(price) as min_price from products where year(launch_date) >= 2025 group by 1;

select brand, min(launch_date) as min_launch_date, min(launch_date) as first_launch_date from products where category in ('Books', 'Home', 'Grocery') group by brand

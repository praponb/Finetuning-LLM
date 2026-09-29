select category, min(launch_date) first_launch_date from products group by category order by first_launch_date asc, category desc;

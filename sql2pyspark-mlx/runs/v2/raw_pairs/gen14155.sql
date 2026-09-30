select year(launch_date) as launch_year, sum(cost) as total_cost, count(*) as cnt, round(sum(cost) * 100.0 / sum(sum(cost)) over (), 2) as pct_of_total from products group by year(launch_date)

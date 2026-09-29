select id, brand, launch_date, cost, lead(cost, 1, 0) over (partition by brand order by launch_date, id) as next_cost from products;

select brand, sum(case when category = 'Home' then 1 else 0 end) as home_count, min(price) as min_price, sum(cost) as total_cost from products group by brand;

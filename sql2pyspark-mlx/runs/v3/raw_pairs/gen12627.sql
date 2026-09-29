select brand, count(price) as price_count, count(case when brand = 'Stark' then 1 end) as stark_count from products where month(launch_date) in (4, 7, 8) group by brand order by brand desc limit 5

select region, count(case when city = 'New York' then 1 end) as new_york_count from stores group by region

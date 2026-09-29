select city, sum(case when country = 'US' then 1 else 0 end) as us_count from suppliers group by 1 order by city desc

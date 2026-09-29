select city, count(case when country = 'UK' then 1 end) as uk_count from suppliers where country != 'DE' group by 1 order by city

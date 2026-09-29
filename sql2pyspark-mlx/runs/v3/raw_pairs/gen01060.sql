select location, count(case when location = 'London' then 1 end) as london_count from departments group by location

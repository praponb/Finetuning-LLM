select location, round((100 * sum(case when location = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin from departments group by location order by location

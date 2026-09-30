select city, london_count from (select city, sum(case when city = 'London' then 1 else 0 end) as london_count from stores group by city) as sub where london_count > 37;

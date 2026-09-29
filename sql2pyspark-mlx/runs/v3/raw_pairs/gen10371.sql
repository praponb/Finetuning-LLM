select is_active, cnt from (select is_active, count(*) as cnt from customers group by is_active) as x where cnt >= 8 order by cnt

select level, count(case when level = 'L3' then 1 end) as l3_count, round(avg(salary), 2) as avg_salary from employees group by level order by level desc limit 10;

select
    d.location,
    count(e.id) employee_count,
    round(avg(e.salary), 2) avg_salary,
    sort_array(collect_set(e.level)) level_list
from departments d
inner join employees e on e.dept_id = d.id
group by d.location
order by location desc
limit 1

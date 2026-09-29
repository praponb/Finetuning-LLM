with employees_by_department as (
    select dept_id, count(case when level = 'L3' then 1 end) l3_count, count(case when level = 'L4' then 1 end) l4_count
    from employees
    where full_name like '%Pim%'
    group by dept_id
)
select d.budget, ebd.l3_count, ebd.l4_count
from departments d
left join employees_by_department ebd on d.id = ebd.dept_id
order by budget desc;

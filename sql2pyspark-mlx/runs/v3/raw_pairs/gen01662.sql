with EmployeesByDepartment as (
    select dept_id, sum(case when level = 'L1' then 1 else 0 end) as l1_count, count(case when level = 'L3' then 1 end) as l3_count
    from employees
    where full_name like 'bob%'
    group by dept_id
)
select d.location, E.l1_count, E.l3_count
from departments d
left join EmployeesByDepartment E on d.id = E.dept_id
where E.l3_count > 4
order by location;

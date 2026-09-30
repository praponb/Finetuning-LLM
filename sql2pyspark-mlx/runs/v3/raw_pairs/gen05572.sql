with TopDepartments as (
    select
        id,
        location,
        budget,
        dept_name,
        row_number() over (partition by location order by budget desc, id) as row_num
    from departments
)
select
    id,
    location,
    budget,
    dept_name
from TopDepartments
where row_num = 1
order by id desc;

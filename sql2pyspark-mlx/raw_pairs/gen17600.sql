with base as (
    -- Keep recent, valid rows
    select *, upper(full_name) as full_name_upper
    from employees
    where salary > 60000
),
deduped as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by dept_id order by hire_date desc) as row_num
    from base
    qualify row_num = 1
),
joined as (
    -- Attach departments attributes
    select
        src.*,
        d.dept_name,
        d.location,
        d.budget
    from deduped src
    left join departments d
        on src.dept_id = d.id
),
with_dims as (
    -- Attach departments attributes
    select
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.manager_id,
        s.job_title,
        s.full_name_upper,
        s.dept_name,
        s.location,
        b.dept_name as department_dept_name,
        b.budget as department_budget,
        b.location as department_location
    from joined s
    left join departments b
        on s.dept_id = b.id
    where b.location not in ('London', 'Singapore', 'Chiang Mai')
),
with_dims_employees as (
    -- Attach employees attributes
    select a.*, p.full_name as employee_full_name
    from with_dims a
    left join employees p
        on a.manager_id <=> p.id
)
-- Join a dimension and aggregate
select
    dim.location as department_location,
    s.dept_name as employee_dept_name,
    count(*) as num_rows,
    min(s.department_budget) as min_department_budget
from with_dims_employees s
join departments dim on s.dept_id = dim.id
group by grouping sets ((dim.location, s.dept_name), (dim.location))
order by num_rows nulls last

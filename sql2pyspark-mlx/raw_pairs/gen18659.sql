with valid as (
    -- Basic cleansing and defaults
    select *, length(dept_name) as dept_name_length
    from departments
    where dept_name is distinct from 'Finance'
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        concat(location, ' - ', dept_name) as location_dept_name,
        coalesce(location, 'NONE') as location_clean
    from valid
    where location in ('London', 'New York', 'Berlin')
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.employee_salary,
        latest.employee_job_title
    from filtered s
    join lateral (
            select c.salary as employee_salary, c.job_title as employee_job_title
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
filtered_departments as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        least(dept_name_length, employee_salary) as least_dept_name_length_employee_salary,
        substring(dept_name, 1, 2) as dept_name_prefix,
        coalesce(location, 'N/A') as location_filled
    from with_latest
    where budget >= 1881000
),
base as (
    -- Keep recent, valid rows
    select
        *,
        upper(location) as location_upper,
        round(dept_name_length / employee_salary, 2) as dept_name_length_per_employee_salary
    from filtered_departments
    where least_dept_name_length_employee_salary >= 74
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select s.*, latest.hire_date
    from base s
    join lateral (
            select c.hire_date
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    -- Ranking and gap features
    select
        id,
        budget,
        dept_name_length,
        location_clean,
        employee_salary,
        employee_job_title,
        least_dept_name_length_employee_salary,
        lag(least_dept_name_length_employee_salary, 1) over (partition by dept_name order by hire_date asc, id asc) as prev_least_dept_name_length_employee_salary,
        ntile(10) over (partition by dept_name order by dept_name_length_per_employee_salary desc, id asc) as dept_name_length_per_employee_salary_bucket
    from latest_match
    where budget <= 1613000
)
select
    id,
    budget,
    location_clean,
    employee_salary,
    least_dept_name_length_employee_salary,
    dept_name_length_per_employee_salary_bucket
from metrics
order by employee_salary desc, least_dept_name_length_employee_salary desc;

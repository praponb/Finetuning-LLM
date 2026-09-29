with base as (
    select
        *,
        initcap(trim(full_name)) as full_name_title,
        upper(level) as level_upper,
        coalesce(job_title, 'DIRECT') as job_title_clean
    from employees
    where hire_date > date '2023-02-22'
      and month(hire_date) in (5, 9, 10)
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by dept_id, manager_id order by salary, id) as rank_in_group
        from base
    ) t
    where rank_in_group = 1
),
remaining_keys as (
    select manager_id, level
    from deduped
    except all
    select manager_id, level
    from employees
    where job_title in ('Director', 'Engineer')
),
reconciled as (
    select s.*
    from deduped s
    join remaining_keys k on s.manager_id = k.manager_id and s.level = k.level
),
with_dims as (
    select e.*, b.location
    from reconciled e
    join departments b
        on e.dept_id = b.id
    where b.dept_name in ('Legal', 'Marketing')
),
with_windows as (
    select
        *,
        rank() over (order by salary desc) as salary_rank,
        round(salary / sum(salary) over (partition by level), 4) as salary_share
    from with_dims
),
valid as (
    select
        *,
        round(salary_rank / salary, 2) as salary_rank_per_salary,
        lower(email) as email_lower,
        coalesce(job_title, 'UNKNOWN') as job_title_filled
    from with_windows
    where hire_date >= date '2023-05-01'
      and level is not null
),
kept_keys as (
    select dept_id, job_title
    from valid
    except distinct
    select dept_id, job_title
    from employees
    where job_title not in ('Analyst', 'Designer')
),
kept_rows as (
    select s.*
    from valid s
    join kept_keys k on s.dept_id = k.dept_id and s.job_title = k.job_title
)
select
    s.id,
    s.full_name,
    s.dept_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.level,
    s.job_title_clean,
    s.salary_rank,
    (select min(x.salary) from employees x where x.dept_id = s.dept_id) as min_employee_salary
from kept_rows s
order by dept_id desc

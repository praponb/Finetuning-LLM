with cleaned as (
    select
        id,
        full_name,
        email,
        salary,
        hire_date,
        is_remote,
        coalesce(level, 'unassigned') as level_filled
    from employees
    where manager_id is not null
      and level is distinct from 'L4'
),
scored as (
    select
        id,
        email,
        is_remote,
        level_filled,
        cume_dist() over (order by salary desc) as salary_cume_dist,
        row_number() over w_running as row_seq,
        avg(salary) over w_part as salary_avg_5_prior
    from cleaned
    window w_running as (order by hire_date, id), w_part as (partition by date_trunc('WEEK', hire_date) order by hire_date, id rows between 5 preceding and 1 preceding)
),
filtered as (
    select *, row_seq + salary_cume_dist as row_seq_plus_salary_cume_dist
    from scored
    where salary_avg_5_prior < 131275
),
scored_employees as (
    select
        id,
        row_seq_plus_salary_cume_dist,
        lead(row_seq_plus_salary_cume_dist) over (order by row_seq_plus_salary_cume_dist, id) as next_row_seq_plus_salary_cume_dist,
        lead(salary_avg_5_prior) over (order by row_seq_plus_salary_cume_dist, id) as next_salary_avg_5_prior,
        lag(salary_avg_5_prior) over (order by row_seq_plus_salary_cume_dist, id) as prev_salary_avg_5_prior,
        row_number() over (order by row_seq_plus_salary_cume_dist, id) as row_seq_2,
        lead(salary_avg_5_prior, 1) over (order by row_seq, id) as next_salary_avg_5_prior_2
    from filtered
)
select
    id,
    row_seq_plus_salary_cume_dist,
    next_row_seq_plus_salary_cume_dist,
    next_salary_avg_5_prior,
    next_salary_avg_5_prior_2
from scored_employees
order by next_row_seq_plus_salary_cume_dist desc, row_seq_plus_salary_cume_dist;

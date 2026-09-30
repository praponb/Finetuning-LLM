with joined as (
    -- Attach departments attributes
    select
        a.id,
        a.email,
        a.dept_id,
        a.salary,
        p.budget,
        p.location,
        p.dept_name
    from employees a
    left join departments p
        on a.dept_id = p.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by location order by budget desc nulls first, id asc) as row_num
    from joined
    qualify row_num = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by dept_name order by budget asc nulls last, id asc) as dedup_rank
    from deduped
    qualify dedup_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by dept_id order by salary asc) as salary_dense_rank,
        row_number() over w_part as seq_no
    from ranked
    where upper(trim(email)) = 'DAVID.LEE@GMAIL.COM'
    window w_part as (partition by dept_id order by salary desc, id asc)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by location order by seq_no asc, id asc) as row_seq,
        sum(salary_dense_rank) over (partition by dept_name, location) as dept_name_sum_salary_dense_rank,
        row_number() over (partition by location order by salary desc, id asc) as seq_no_2,
        ntile(3) over (order by salary_dense_rank asc, id asc) as salary_dense_rank_bucket,
        avg(salary_dense_rank) over w_recent as salary_dense_rank_avg_3_prior
    from with_windows
    window w_recent as (partition by dept_name order by budget asc, id asc rows between 3 preceding and 1 preceding)
)
select
    id,
    salary,
    budget,
    location,
    row_num,
    dedup_rank,
    salary_dense_rank,
    seq_no,
    dept_name_sum_salary_dense_rank,
    case when location rlike '^(New York|Tokyo)$' then 'GROUP_A' when location rlike '^(Bangkok|Chiang Mai)$' then 'CORE' else 'GROUP_B' end as location_zone
from scored

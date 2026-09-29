select is_remote, salary_bucket, l1_count
from (
    select is_remote, case when salary >= 146000 then 'high' when salary >= 64000 then 'mid' else 'low' end as salary_bucket, count(case when level = 'L1' then 1 end) as l1_count
    from employees
    where dept_id in (1, 22, 33)
    group by is_remote, case when salary >= 146000 then 'high' when salary >= 64000 then 'mid' else 'low' end
) as agg
where l1_count > 8
order by salary_bucket, 3, is_remote
limit 5;

select case when salary >= 159000 then 'high' when salary >= 41000 then 'mid' else 'low' end as salary_bucket, sort_array(collect_set(level)) as level_list
from employees
where (is_remote and level = 'L3') or job_title is null
group by 1
order by salary_bucket desc;

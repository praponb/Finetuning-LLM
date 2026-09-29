select is_remote, count(case when job_title = 'Designer' then 1 end) designer_count from employees group by is_remote having designer_count > 2 order by designer_count desc;

select * from (select *, row_number() over (partition by level order by salary, id) as rn from employees) as ranked where rn <= 3 order by job_title desc;

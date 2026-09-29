select job_title, sum(case when level = 'L3' then salary else 0 end) l3_salary from employees group by job_title having sum(case when level = 'L3' then salary else 0 end) > 45 order by job_title asc;

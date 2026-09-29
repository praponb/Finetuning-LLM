select x.segment, y.job_title from (select distinct segment from customers) x cross join (select distinct job_title from employees) y order by segment, job_title

select x.city, y.job_title from (select distinct city from customers) x cross join (select distinct job_title from employees) y order by city, job_title

select x.country, y.job_title from (select distinct country from customers) x cross join (select distinct job_title from employees) y;

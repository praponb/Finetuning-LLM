select full_name from employees where job_title = 'Analyst' union select name as full_name from customers where age < 59 order by full_name desc;

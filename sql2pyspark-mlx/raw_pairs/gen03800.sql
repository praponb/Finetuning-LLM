select date_format(hire_date, 'yyyy-MM') as hire_month, sum(case when level = 'L2' then salary else 0 end) as l2_salary from employees group by hire_month order by hire_month, l2_salary limit 20;

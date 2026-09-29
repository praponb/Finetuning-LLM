select sum(case when dept_name = 'Marketing' then budget else 0 end) as marketing_budget from departments order by marketing_budget;

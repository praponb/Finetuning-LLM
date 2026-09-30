select * from departments where dept_name in ('Engineering', 'Sales') and dept_name is not null and location in ('Tokyo', 'Bangkok', 'Chiang Mai') order by id desc

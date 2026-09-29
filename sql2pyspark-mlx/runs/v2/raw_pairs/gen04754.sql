select * from departments where budget > 2266000 and (budget <= 2113000 or dept_name in ('Engineering', 'Marketing', 'HR')) order by id desc, location;

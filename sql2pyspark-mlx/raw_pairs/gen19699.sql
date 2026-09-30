select dept_name, 'department' as kind from departments where dept_name not in ('Sales', 'Finance', 'Engineering') union select region, 'store' as kind from stores order by dept_name desc;

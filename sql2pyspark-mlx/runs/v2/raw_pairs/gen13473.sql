select dept_id from employees where lower(trim(full_name)) = 'jack taylor' union all select dept_id from employees where level not in ('L2', 'L4', 'L1');

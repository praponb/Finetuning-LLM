select id, is_remote, full_name, floor(salary) as salary_whole from employees where not (length(email) > 10 and lower(email) like '%ack%' and level in ('L3', 'L4', 'L1'));

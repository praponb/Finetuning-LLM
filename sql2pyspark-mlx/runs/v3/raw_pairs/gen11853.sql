select id, salary, level, manager_id from employees where not (length(email) >= 11 and (manager_id in (18, 36) or not is_remote)) order by salary, id;

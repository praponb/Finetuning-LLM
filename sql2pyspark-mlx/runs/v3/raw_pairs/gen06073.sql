select id, manager_id, split(lower(email), '@')[0] email_user from employees where not is_remote

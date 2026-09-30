select *, split(lower(email), '@')[0] email_user, if(is_remote, 'Yes', 'No') is_remote_label
from employees
where dept_id in (17, 23, 33) and dept_id in (1, 16, 20, 26)

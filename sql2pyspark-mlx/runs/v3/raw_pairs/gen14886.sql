select *, concat(level, ' ', full_name) level_full_name, initcap(trim(email)) email_title
from employees
where email like '%@yahoo.com'
order by manager_id desc;

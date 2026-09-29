select location, dept_name, concat(location, ' ', dept_name) as location_dept_name
from departments
where not ((budget < 3968000 and dept_name <> 'Sales') or budget between 2476000 and 2687000)

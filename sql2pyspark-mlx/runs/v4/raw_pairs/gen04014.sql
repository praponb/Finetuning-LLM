select
    location,
    dept_name,
    case when dept_name = 'Legal' then 1 else 0 end as is_legal,
    concat_ws(',', dept_name, location) as dept_name_location_key
from departments;

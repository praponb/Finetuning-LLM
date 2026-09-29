select budget, coalesce(budget, 0) as budget_filled, concat(location, ', ', dept_name) as location_dept_name from departments order by location_dept_name desc;

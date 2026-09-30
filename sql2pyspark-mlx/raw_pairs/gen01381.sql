select location, dept_name, coalesce(location, 'unassigned') as location_filled from departments where dept_name in ('Finance', 'HR');

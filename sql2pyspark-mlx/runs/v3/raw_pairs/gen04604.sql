select budget, dept_name, location, coalesce(location, 'Unknown') as location_filled from departments where location in ('Tokyo', 'Singapore') and location is not null

select dept_name, location, upper(location) as location_upper from departments where not (location != 'Tokyo' and budget >= 2566000 and location is null)

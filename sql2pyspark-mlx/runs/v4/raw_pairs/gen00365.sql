SELECT id, location, SUBSTRING(location, 1, 4) location_prefix, COALESCE(location, 'Unknown') location_filled FROM departments

select budget, coalesce(location, 'Unknown') as location_filled from departments where budget <= 3210000 and location = 'Berlin' order by budget desc

SELECT browser, country, COALESCE(country, 'unassigned') AS country_filled FROM sessions WHERE browser IN ('Edge', 'Firefox') AND (country = 'DE' OR customer_id = 33)

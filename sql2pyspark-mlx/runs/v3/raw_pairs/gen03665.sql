SELECT id, device, LOWER(country) AS country_lower, CASE WHEN device = 'desktop' THEN 1 ELSE 0 END AS is_desktop FROM sessions ORDER BY device ASC, id ASC

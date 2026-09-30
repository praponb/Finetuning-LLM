SELECT * FROM sessions WHERE device NOT IN ('desktop', 'mobile', 'tablet') AND browser IN ('Firefox', 'Chrome') ORDER BY 2, id LIMIT 50;
